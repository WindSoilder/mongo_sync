use super::full::{sync_one_concurrent, sync_one_serial, SyncTableStatus};
use super::{bson_helper, mongo_helper, oplog_helper, time_helper};
use crate::blocking::connection::Connection;
use crate::error::{Result, SyncError};
use crate::{NAMESPACE_KEY, TIMESTAMP_KEY};
use bson::{doc, Bson, Document, Timestamp};
use crossbeam::channel;
use mongodb::options::{FindOneOptions, UpdateOptions};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

pub struct MongoSyncer {
    manager: SyncManager,
}

const LARGE_COLL_SIZE: usize = 10000;

impl MongoSyncer {
    pub fn new(conn: Connection) -> MongoSyncer {
        MongoSyncer {
            manager: SyncManager::new(conn),
        }
    }

    pub fn sync_full(&self) -> Result<()> {
        self.manager.sync_full()
    }

    pub fn sync_incremental(&self) -> Result<()> {
        self.manager.sync_incr_forever()
    }

    pub fn sync(self) -> Result<()> {
        // check time record missing.
        if self.manager.is_time_record_missing()? {
            self.sync_full()?;
        } else {
            // Although we are in increment state, but we may need to full sync some collection.
            // Example:
            // The first time we only want to sync collection `A`, `B`.
            // But this time we want to sync collection `A`, `B`, `C` (or full DB).
            if let Some(new_colls) = self.manager.get_new_colls_to_sync()? {
                info!(
                    ?new_colls,
                    "Get new collections to sync, apply oplogs until now. "
                );
                self.manager.sync_incr_to_now()?;
                info!(?new_colls, "Make full sync for new collections. ");
                self.manager.sync_documents_for_collections(&new_colls)?;
                info!(?new_colls, "Full sync for new collections complete, goes into incremental node. ");
            }
        }
        // record sync collection arguments.
        self.manager.write_sync_colls_args()?;
        self.sync_incremental()
    }
}

struct SyncManager {
    conn: Connection,
    pool: ThreadPool,
    coll_sync_pool: Arc<ThreadPool>,
}

impl SyncManager {
    pub fn new(conn: Connection) -> SyncManager {
        let conf = conn.get_conf();
        let coll_concurrent = conf.get_collection_concurrent();
        let doc_concurrent = conf.get_doc_concurrent();
        SyncManager {
            conn,
            coll_sync_pool: Arc::new(
                ThreadPoolBuilder::new()
                    .num_threads(doc_concurrent)
                    .build()
                    .unwrap(),
            ),
            pool: ThreadPoolBuilder::new()
                .num_threads(coll_concurrent)
                .build()
                .unwrap(),
        }
    }

    pub fn sync_full(&self) -> Result<()> {
        let oplog_start = oplog_helper::get_latest_ts_no_capped(&self.conn.oplog_coll())?;

        // just use for log..
        let start_time = time_helper::to_datetime(&oplog_start);
        info!(%start_time, "Full state: begin to sync databases. ");
        self.sync_documents_full()?;
        info!(%start_time, "Full state: sync database complete, check oplog and write start point.");

        if self.check_log_valid(oplog_start)? {
            self.write_log_record(oplog_start)?;
            info!(%start_time, "Full state: write oplog start point complete, goes into incremental mode.")
        } else {
            panic!("Full state: oplog is no-longer valid, because they are not exists in database");
        }

        Ok(())
    }

    pub fn is_time_record_missing(&self) -> Result<bool> {
        // when the following happened, return true:
        // 1. can't get time record information, or
        // 2. the minimum value of oplog timestamp > time record.
        // condition 2 means that oplog can't be applied between time record and min(oplog timestamp)
        let coll = self.conn.time_record_coll();
        let rec = coll.find_one(None, None)?;
        match rec {
            Some(doc) => {
                let ts = doc.get_timestamp(TIMESTAMP_KEY)?;
                let missing = self
                    .conn
                    .oplog_coll()
                    .find_one(
                        None,
                        FindOneOptions::builder().sort(doc! {"$natural": 1}).build(),
                    )?
                    .map(|log| {
                        // unwrap here is ok because oplog always contains `ts` field.
                        let oldest_ts = log.get_timestamp(TIMESTAMP_KEY).unwrap();
                        oldest_ts > ts
                    });
                match missing {
                    Some(missed) => Ok(missed),
                    None => Ok(false), // can't find oplog.
                }
            }
            None => Ok(true),
        }
    }

    fn sync_incr(&self, forever: bool) -> Result<()> {
        let oplog_coll = self.conn.oplog_coll();
        let sleep_secs = std::time::Duration::from_secs(3);
        let uuid_mapping = self.get_uuid_mapping()?;

        let src_db = self.conn.get_src_db();
        let uuids = mongo_helper::get_uuids(&src_db, self.conn.get_conf().get_colls())?;
        loop {
            // For every loop we just sleep 3 seconds, to make less frequency query to mongodb.
            std::thread::sleep(sleep_secs);
            let start_point = self
                .conn
                .time_record_coll()
                .find_one(None, None)?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)
                .expect("oplog should contains a key named `ts`");

            let end_point = oplog_coll
                .find_one(
                    doc! {},
                    FindOneOptions::builder()
                        .sort(doc! {TIMESTAMP_KEY: -1})
                        .build(),
                )?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)
                .expect("oplog should contains a key named `ts`");

            if start_point < end_point {
                // only used for log...
                let start_time = time_helper::to_datetime(&start_point);
                let end_time = time_helper::to_datetime(&end_point);

                info!(%start_time, "Begin fetch oplog");
                let mut oplogs = self.fetch_oplogs(start_point, end_point)?;
                info!(%end_time, "fetch oplog complete");
                bson_helper::map_oplog_uuids(&mut oplogs, &uuid_mapping)?;

                if !oplogs.is_empty() {
                    info!(%start_time, %end_time, "Incr state: Filter and apply oplogs ");
                    let oplogs = self.filter_oplogs(oplogs, &uuids);
                    self.apply_logs(oplogs)?;
                    info!(%start_time, %end_time, "Incr state: Apply oplogs complete ");
                }
                info!(%end_time, "Incr state: Write oplog records");
                self.write_log_record(end_point)?;
            } else if start_point == end_point {
                info!("Incr state: No new oplogs available here, continue..");
            } else {
                // TODO: start_point > end_point, data corrupted occured, handle for this.
            }

            if !forever {
                return Ok(());
            }
        }
    }

    // TODO: when `end_ts` - `start_ts` too large, fetch oplogs may goes into fail.
    fn fetch_oplogs(&self, start_ts: Timestamp, end_ts: Timestamp) -> Result<Vec<Document>> {
        let cursor = self
            .conn
            .oplog_coll()
            .find(doc! {"ts": {"$gte": start_ts, "$lte": end_ts}}, None)?;

        let mut result = vec![];
        for doc in cursor {
            let doc = doc?;
            result.push(doc)
        }
        Ok(result)
    }

    fn filter_oplogs(&self, oplogs: Vec<Document>, valid_uuids: &HashSet<Uuid>) -> Vec<Document> {
        let conf = self.conn.get_conf();
        let sync_db = conf.get_db();
        match self.conn.get_conf().get_colls() {
            // just need to filter oplogs which namespaces starts by 'given db'
            None => oplogs
                .into_iter()
                .filter(|x| x.get_str(NAMESPACE_KEY).unwrap().starts_with(sync_db))
                .collect(),
            Some(_) => oplogs
                .into_iter()
                .filter(|x| {
                    // filter by oplog 'ui'(collection uuid) field.
                    // The benefit of using this field, we don't need to handle special case
                    // like collection rename and insert.
                    valid_uuids.contains(
                        &bson_helper::get_uuid(x, "ui")
                            .expect("oplog item should contains 'ui' field."),
                    )
                })
                .collect(),
        }
    }

    fn check_log_valid(&self, start_point: Timestamp) -> Result<bool> {
        // just fetch oplog start point, if the start point is less than given oplogs, we can make sure that these oplogs is still valid.
        let earliest_ts = oplog_helper::get_earliest_ts_no_capped(&self.conn.oplog_coll())?;
        Ok(earliest_ts < start_point)
    }

    fn apply_logs(&self, oplogs: Vec<Document>) -> Result<()> {
        let _exec_result = self
            .conn
            .get_target_admin_db()
            .run_command(doc! {"applyOps": oplogs}, None)
            .map_err(|e| SyncError::MongoError(e))?;
        // TODO: parse running result.
        Ok(())
    }

    fn write_log_record(&self, log_ts: Timestamp) -> Result<()> {
        self.conn.time_record_coll().update_one(
            doc! {},
            doc! { "$set": {"ts": log_ts} },
            UpdateOptions::builder().upsert(true).build(),
        )?;
        Ok(())
    }

    fn sync_documents_full(&self) -> Result<()> {
        let src_db = self.conn.get_src_db();
        let coll_names = match self.conn.get_conf().get_colls() {
            // use unwrap here is ok, because we have check list_collection_names before.
            None => src_db.list_collection_names(None).unwrap(),
            Some(colls) => colls.clone(),
        };

        self.sync_documents_for_collections(&coll_names)
    }

    pub fn sync_incr_to_now(&self) -> Result<()> {
        self.sync_incr(false)
    }

    pub fn sync_incr_forever(&self) -> Result<()> {
        self.sync_incr(true)
    }

    pub fn sync_documents_for_collections(&self, coll_names: &[String]) -> Result<()> {
        let conf = self.conn.get_conf();
        let coll_concurrent = conf.get_collection_concurrent();
        let doc_concurrent = conf.get_doc_concurrent();
        let (sender, receiver) = channel::bounded(coll_concurrent);
        let (src_db, target_db) = (self.conn.get_src_db(), self.conn.get_target_db());

        let total = coll_names.len();
        for coll in coll_names.iter() {
            let sender = sender.clone();
            let source_coll = src_db.collection(coll);
            let target_coll = target_db.collection(coll);
            let doc_count = source_coll.estimated_document_count(None)? as usize;
            target_coll.drop(None)?;

            if doc_count <= LARGE_COLL_SIZE {
                self.pool.spawn(move || {
                    if let Err(e) = sync_one_serial(source_coll, target_coll) {
                        let _ = sender.send(SyncTableStatus::Failed(e));
                    }
                    let _ = sender.send(SyncTableStatus::Done);
                })
            } else {
                let coll_pool = self.coll_sync_pool.clone();
                self.pool.spawn(move || {
                    if let Err(e) =
                        sync_one_concurrent(source_coll, target_coll, doc_concurrent, coll_pool)
                    {
                        let _ = sender.send(SyncTableStatus::Failed(e));
                    }
                    let _ = sender.send(SyncTableStatus::Done);
                })
            }
        }

        let mut complete_count = 0;
        while let Ok(event) = receiver.recv() {
            match event {
                SyncTableStatus::Done => {
                    complete_count += 1;
                    if total == complete_count {
                        break;
                    }
                }
                SyncTableStatus::Failed(e) => {
                    return Err(e);
                }
            }
        }

        // We rebuild index in this main thread, because build index operation seems like to lock the whole database
        // and build index in the sub-thread is meanless.
        info!("Full state: Begin to re-build index for target collection");
        for coll in coll_names.iter() {
            let indexes = src_db.run_command(doc! { "listIndexes": coll }, None)?;
            let indexes = indexes.get_document("cursor")?.get_array("firstBatch")?;
            // FIXME: will have problem when we have many indexes, using firstBatch is not enough.
            target_db.run_command(
                doc! {
                    "createIndexes": coll,
                    "indexes": indexes,
                },
                None,
            )?;
        }
        Ok(())
    }

    // generate uuid mapping between source database collection and target database collection
    // we need this mapping to make `applyOps` command works.
    fn get_uuid_mapping(&self) -> Result<HashMap<Uuid, Uuid>> {
        let (src_db, target_db) = (self.conn.get_src_db(), self.conn.get_target_db());
        mongo_helper::get_uuid_mapping(&src_db, &target_db)
    }

    pub fn write_sync_colls_args(&self) -> Result<()> {
        let target_db = self.conn.get_target_db();
        let colls_to_sync = target_db.collection("colls_to_sync");
        match self.conn.get_conf().get_colls() {
            None => {
                colls_to_sync.delete_many(doc! {}, None)?;
            }
            Some(coll_names) => {
                colls_to_sync.delete_many(doc! {}, None)?;
                colls_to_sync.insert_one(doc! {"names": coll_names}, None)?;
            }
        }
        Ok(())
    }

    fn get_previous_sync_colls_args(&self) -> Result<Option<HashSet<String>>> {
        let target_db = self.conn.get_target_db();
        let colls_to_sync = target_db.collection("colls_to_sync");
        let item = colls_to_sync.find_one(doc! {}, None)?;
        match item {
            None => Ok(None),
            Some(d) => Ok(Some(
                d.get_array("names")
                    .unwrap()
                    .iter()
                    .map(|x| match x {
                        Bson::String(s) => s.clone(),
                        _ => panic!("xxx"),
                    })
                    .collect(),
            )),
        }
    }

    pub fn get_new_colls_to_sync(&self) -> Result<Option<Vec<String>>> {
        let origin_sync_colls = self.get_previous_sync_colls_args()?;
        match origin_sync_colls {
            None => Ok(None),
            Some(origin_colls) => {
                let src_db = self.conn.get_src_db();
                let current_sync_colls: HashSet<String> = match self.conn.get_conf().get_colls() {
                    // use unwrap here is ok, because we have check list_collection_names before.
                    None => src_db
                        .list_collection_names(None)
                        .unwrap()
                        .into_iter()
                        .collect(),
                    Some(colls) => colls.iter().map(|x| x.clone()).collect(),
                };
                let new_colls: Vec<String> = current_sync_colls
                    .difference(&origin_colls)
                    .map(|x| x.clone())
                    .collect();
                if new_colls.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(new_colls))
                }
            }
        }
    }
}
