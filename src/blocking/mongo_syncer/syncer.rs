use super::full::{sync_one_concurrent, sync_one_serial, SyncTableStatus};
use super::oplog_helper;
use crate::blocking::connection::Connection;
use crate::error::{Result, SyncError};
use crate::{NAMESPACE_KEY, TIMESTAMP_KEY};
use bson::{doc, Document, Timestamp};
use crossbeam::channel::{self, Receiver, Sender};
use mongodb::options::{FindOneOptions, UpdateOptions};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;
use tracing::info;

pub struct MongoSyncer {
    manager: SyncManager,
    receiver: Receiver<ManagerTaskStatus>,
}

const LARGE_COLL_SIZE: usize = 10000;

impl MongoSyncer {
    pub fn new(conn: Connection) -> MongoSyncer {
        let (sender, receiver) = channel::bounded(1);
        MongoSyncer {
            manager: SyncManager::new(conn, sender),
            receiver,
        }
    }

    pub fn sync_full(&self) -> Result<()> {
        self.manager.sync_full()?;

        match self.receiver.recv() {
            Ok(_) => Ok(()),
            Err(e) => Err(SyncError::ReceiveStatusError(e)),
        }
    }

    pub fn sync_incremental(&self) -> Result<()> {
        self.manager.sync_incr()
    }

    pub fn sync(self) -> Result<()> {
        loop {
            // check time record missing.
            if self.manager.is_time_record_missing()? {
                self.sync_full()?;
            }
            // self.manager.write_time_record()?;
            self.sync_incremental()?;
        }
    }
}

enum ManagerTaskStatus {
    Done,
}

struct SyncManager {
    conn: Connection,
    pool: ThreadPool,
    coll_sync_pool: Arc<ThreadPool>,
    sender: Sender<ManagerTaskStatus>,
}

impl SyncManager {
    pub fn new(conn: Connection, sender: Sender<ManagerTaskStatus>) -> SyncManager {
        let conf = conn.get_conf();
        let coll_concurrent = conf.get_collection_concurrent();
        let doc_concurrent = conf.get_doc_concurrent();
        SyncManager {
            conn,
            sender,
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
        info!("Full state: begin to sync databases");
        self.sync_documents_full()?;
        info!("Full state: sync database complete, begin to apply oplogs.");
        let oplog_end = oplog_helper::get_latest_ts_no_capped(&self.conn.oplog_coll())?;
        info!("Full state: begin to fetch oplogs");
        let oplogs = self.fetch_oplogs(oplog_start, oplog_end)?;
        let oplogs = self.filter_oplogs(oplogs);
        info!("Full state: Fetch oplog complete");
        if !oplogs.is_empty() {
            info!("Full state: Filter and apply oplogs");
            self.check_logs_valid(&oplogs)?;
            self.apply_logs(oplogs)?;
            info!("Full state: Apply oplogs complete");
        }
        self.write_log_record(oplog_end)?;
        info!("Full state: Write oplog end point complete, goes into incremental mode");
        Ok(())
    }

    fn filter_oplogs(&self, oplogs: Vec<Document>) -> Vec<Document> {
        let conf = self.conn.get_conf();
        let sync_db = conf.get_db();
        match self.conn.get_conf().get_colls() {
            // just need to filter oplogs which namespaces starts by 'given db'
            None => oplogs
                .into_iter()
                .filter(|x| x.get_str(NAMESPACE_KEY).unwrap().starts_with(sync_db))
                .collect(),
            Some(colls) => oplogs
                .into_iter()
                .filter(|x| {
                    let (db_name, coll_name) =
                        x.get_str(NAMESPACE_KEY).unwrap().split_once(".").unwrap();
                    db_name == sync_db
                        && (coll_name == "$cmd" || colls.iter().any(|x| x == coll_name))
                })
                .collect(),
        }
    }

    fn sync_documents_full(&self) -> Result<()> {
        let conf = self.conn.get_conf();
        let coll_concurrent = conf.get_collection_concurrent();
        let doc_concurrent = conf.get_doc_concurrent();
        let (sender, receiver) = channel::bounded(coll_concurrent);
        let (src_db, target_db) = (self.conn.get_src_db(), self.conn.get_target_db());

        let coll_names = match self.conn.get_conf().get_colls() {
            // use unwrap here is ok, because we have check list_collection_names before.
            None => src_db.list_collection_names(None).unwrap(),
            Some(colls) => colls.clone(),
        };
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
                        let _ = self.sender.send(ManagerTaskStatus::Done);
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

    pub fn sync_incr(&self) -> Result<()> {
        let oplog_coll = self.conn.oplog_coll();
        let sleep_secs = std::time::Duration::from_secs(3);

        loop {
            let start_point = self
                .conn
                .time_record_coll()
                .find_one(None, None)?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)
                .unwrap();
            let end_point = oplog_coll
                .find_one(
                    doc! {},
                    FindOneOptions::builder()
                        .sort(doc! {TIMESTAMP_KEY: -1})
                        .build(),
                )?
                .unwrap()
                .get_timestamp(TIMESTAMP_KEY)
                .unwrap();

            let oplogs = self.fetch_oplogs(start_point, end_point)?;
            if !oplogs.is_empty() {
                info!("Incr state: Filter and apply oplogs");
                let oplogs = self.filter_oplogs(oplogs);
                self.apply_logs(oplogs)?;
                info!("Incr state: Apply oplogs complete");
            }
            self.write_log_record(end_point)?;
            // For every loop we just sleep 3 seconds, to make less frequency query to mongodb.
            std::thread::sleep(sleep_secs);
        }
    }

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

    fn check_logs_valid(&self, oplogs: &[Document]) -> Result<bool> {
        // just fetch oplog start point, if the start point is less than given oplogs, we can make sure that these oplogs is still valid.
        let earliest_ts = oplog_helper::get_earliest_ts_no_capped(&self.conn.oplog_coll())?;
        Ok(earliest_ts < oplogs[0].get_timestamp(TIMESTAMP_KEY)?)
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
}
