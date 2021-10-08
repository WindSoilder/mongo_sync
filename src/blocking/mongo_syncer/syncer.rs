use super::full::{sync_one_concurrent, sync_one_serial, SyncTableStatus};
use super::incr::IncrDumper;
use super::oplog_helper;
use crate::blocking::connection::Connection;
use crate::error::Result;
use crate::{DbSyncConf, TIMESTAMP_KEY};
use bson::{doc, Bson, Document, Timestamp};
use crossbeam::channel;
use mongodb::options::{FindOneOptions, UpdateOptions};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

/// Mongodb syncer to sync from one database to another database.
///
/// # Example
/// ```no_run
/// use mongo_sync::{DbSyncConf, MongoSyncer};
///
/// let conf = DbSyncConf::new("mongodb://localhost:27017".to_string(), "mongodb://localhost:27018".to_string(), "mongodb://localhost:27017".to_string(), "a".to_string(), None, None, None);
/// let syncer = MongoSyncer::new(&conf);
/// syncer.sync();
/// ```
pub struct MongoSyncer<'a> {
    conf: &'a DbSyncConf,
}

const LARGE_COLL_SIZE: usize = 10000;

impl<'a> MongoSyncer<'a> {
    /// create a new Syncer according to given `conf`.
    pub fn new(conf: &DbSyncConf) -> MongoSyncer {
        MongoSyncer { conf }
    }

    /// go and sync databse forever.
    pub fn sync(self) -> Result<()> {
        // Full sync stage.
        {
            let connection = Connection::new(self.conf)?;
            let manager = SyncManager::new(connection);
            // check time record missing.
            if manager.is_time_record_missing()? {
                manager.sync_full()?;
            } else {
                // Although we are in increment state, but we may need to full sync some collection.
                // Example:
                // The first time we only want to sync collection `A`, `B`.
                // But this time we want to sync collection `A`, `B`, `C` (or full DB).
                if let Some(new_colls) = manager.get_new_colls_to_sync()? {
                    info!(
                        ?new_colls,
                        "Get new collections to sync, apply oplogs until now. "
                    );
                    manager.sync_incr_to_now()?;
                    info!(?new_colls, "Make full sync for new collections. ");
                    manager.sync_documents_for_collections(&new_colls)?;
                    info!(
                        ?new_colls,
                        "Full sync for new collections complete, goes into incremental node. "
                    );
                }
            }
            // record sync collection arguments.
            manager.write_sync_colls_args()?;
        }

        // Incremental sync stage
        {
            let connection = Connection::new(self.conf)?;
            let manager = SyncManager::new(connection);

            manager.sync_incr_forever()
        }
    }
}

struct SyncManager<'a> {
    conn: Connection<'a>,
    pool: ThreadPool,
    coll_sync_pool: Arc<ThreadPool>,
}

impl<'a> SyncManager<'a> {
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

    /// make a full sync progress.
    pub fn sync_full(&self) -> Result<()> {
        let oplog_start = oplog_helper::get_latest_ts_no_capped(&self.conn.oplog_coll())?;

        info!(?oplog_start, "Full state: begin to sync databases. ");
        self.sync_documents_full()?;
        info!(
            ?oplog_start,
            "Full state: sync database complete, check oplog and write start point."
        );

        if self.check_log_valid(oplog_start)? {
            self.write_log_record(oplog_start)?;
            info!(
                ?oplog_start,
                "Full state: write oplog start point complete, goes into incremental mode."
            )
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
        let mut sleep_secs = std::time::Duration::from_secs(3);

        let target_client = self.conn.get_target_client();
        let mut incr_dumper = IncrDumper::new(target_client);

        // it's only useful when we don't want to sync forever.
        // When we don't want to sync forever, we just want to apply oplog until this end_point.
        let original_end_point = if !forever {
            oplog_coll
                .find_one(
                    doc! {},
                    FindOneOptions::builder()
                        .sort(doc! {TIMESTAMP_KEY: -1})
                        .build(),
                )?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)?
        } else {
            Timestamp {
                time: 0,
                increment: 0,
            }
        };
        let sync_db_name = self.conn.get_conf().get_db();
        let colls_to_sync = self.get_sync_coll_args()?;

        loop {
            if !sleep_secs.is_zero() {
                std::thread::sleep(sleep_secs);
            }
            let start_point = self
                .conn
                .time_record_coll()
                .find_one(None, None)?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)?;

            let mut end_point = None;
            if !forever {
                end_point = Some(original_end_point);
            }

            info!(?start_point, ?end_point, "Incr state: Begin fetch oplog. ");
            let oplogs = oplog_helper::get_next_batch(&oplog_coll, start_point, end_point, 10000)?;
            if oplogs.is_empty() {
                info!("Incr state: No new oplogs available here, continue..");
                if !forever {
                    return Ok(());
                }
                sleep_secs = std::time::Duration::from_secs(3);
                continue;
            } else if oplogs.len() < 1000 {
                sleep_secs = std::time::Duration::from_secs(2);
            } else {
                sleep_secs = std::time::Duration::from_secs(0);
            }

            let latest_oplog_time = oplogs[oplogs.len() - 1].get_timestamp(TIMESTAMP_KEY)?;
            let oplogs = oplog_helper::filter_oplogs(oplogs, sync_db_name, &colls_to_sync);
            if !oplogs.is_empty() {
                info!(
                    ?start_point,
                    ?end_point,
                    "Incr state: fetch oplog complete. Length of oplogs: {}",
                    oplogs.len()
                );
                incr_dumper.push_oplogs(oplogs);
                loop {
                    let (need_apply_again, latest_applied_ts) = incr_dumper.apply_oplogs()?;
                    self.write_log_record(latest_applied_ts)?;
                    if !need_apply_again {
                        break
                    }
                }
                info!(?end_point, "Incr state: Write oplog records");
            } else {
                info!("Incr state: have fetch oplogs, but all of them is meant to be filtered.");
            }
            self.write_log_record(latest_oplog_time)?;
            if !forever && latest_oplog_time == original_end_point {
                return Ok(());
            }
        }
    }

    fn check_log_valid(&self, start_point: Timestamp) -> Result<bool> {
        // just fetch oplog start point, if the start point is less than given oplogs, we can make sure that these oplogs is still valid.
        let earliest_ts = oplog_helper::get_earliest_ts_no_capped(&self.conn.oplog_coll())?;
        Ok(earliest_ts < start_point)
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

        if !coll_names.is_empty() {
            self.sync_documents_for_collections(&coll_names)
        } else {
            info!("Full state: no collections in database, done.");
            Ok(())
        }
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
            // TODO: will have problem when we have many indexes, using firstBatch is not enough, refer to mongodb document:
            // https://docs.mongodb.com/manual/reference/command/listIndexes/
            // A document that contains information with which to create a cursor to index information. The cursor information includes the cursor id, the
            // full namespace for the command, as well as the first batch of results. Index information includes the keys and options used to create the index.
            // and we have no way to fix it for now, because mongodb-driver doesn't provide something like `command_cursor`.
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

    fn get_sync_coll_args(&self) -> Result<Option<HashSet<String>>> {
        let target_db = self.conn.get_target_db();
        let colls_to_sync = target_db.collection::<Document>("colls_to_sync");
        let item = colls_to_sync.find_one(doc! {}, None)?;
        match item {
            None => Ok(None),
            Some(d) => Ok(Some(
                d.get_array("names")
                    .unwrap()
                    .iter()
                    .map(|x| match x {
                        Bson::String(s) => s.clone(),
                        _ => panic!(r#"The elements in `names` fields should be string, data corrupted!!"
"Try drop `colls_to_sync`, `oplog_records` collection in target_database, and make a full sync again."#),
                    })
                    .collect(),
            )),
        }
    }

    pub fn get_new_colls_to_sync(&self) -> Result<Option<Vec<String>>> {
        let origin_sync_colls = self.get_sync_coll_args()?;
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
                    Some(colls) => colls.iter().cloned().collect(),
                };
                let new_colls: Vec<String> = current_sync_colls
                    .difference(&origin_colls)
                    .cloned()
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
