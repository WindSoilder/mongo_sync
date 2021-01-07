use super::full::{sync_one_concurrent, sync_one_serial, SyncTableStatus};
use crate::blocking::connection::Connection;
use crate::error::{Result, SyncError};
use bson::doc;
use crossbeam::channel::{self, Receiver, Sender};
use mongodb::options::FindOneOptions;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::Arc;

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
        unimplemented!()
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
        Ok(())
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

        for coll in coll_names.into_iter() {
            let sender = sender.clone();
            let source_coll = src_db.collection(&coll);
            let target_coll = target_db.collection(&coll);
            // ??? Maybe we need to put these operation into threads.
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
                let ts = doc.get_timestamp("ts")?;
                let missing = self
                    .conn
                    .oplog_coll()
                    .find_one(None, FindOneOptions::builder().sort(doc! {"ts": 1}).build())?
                    .map(|log| {
                        // unwrap here is ok because oplog always contains `ts` field.
                        let oldest_ts = log.get_timestamp("ts").unwrap();
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
        unimplemented!()
    }
}
