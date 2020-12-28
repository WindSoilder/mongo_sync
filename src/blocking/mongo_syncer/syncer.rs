use crate::blocking::connection::Connection;
use crate::error::{Result, SyncError};
use bson::doc;
use bson::oid::ObjectId;
use crossbeam::channel::{self, Receiver, Sender};
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::sync::Collection;
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
        self.sync_full()?;
        // self.sync_incremental();
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

enum SyncTableStatus {
    Done,
    Failed(SyncError),
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

        let coll_names = match self.conn.get_conf().get_colls() {
            // use unwrap here is ok, because we have check list_collection_names before.
            None => self.conn.get_src_db().list_collection_names(None).unwrap(),
            Some(colls) => colls.clone(),
        };
        let total = coll_names.len();

        for coll in coll_names.into_iter() {
            let sender = sender.clone();
            let source_coll = self.conn.get_src_db().collection(&coll);
            let target_coll = self.conn.get_target_db().collection(&coll);
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

    pub fn sync_incr(&self) -> Result<()> {
        unimplemented!()
    }
}

fn sync_one_concurrent(
    source_coll: Collection,
    target_coll: Collection,
    doc_concurrent: usize,
    pool: Arc<ThreadPool>,
) -> Result<()> {
    // get ObjectId range for each threads.
    let id_ranges: Vec<(ObjectId, ObjectId)> = split_ids(&source_coll, doc_concurrent)?;
    let buf_size = 10000;
    let (sender, receiver) = channel::bounded(doc_concurrent);
    for (id_min, id_max) in id_ranges {
        let source_coll = source_coll.clone();
        let target_coll = target_coll.clone();
        let sender = sender.clone();
        pool.spawn(move || {
            let mut buffer = Vec::with_capacity(buf_size);
            let res = source_coll
                .find(
                    Some(doc! {"_id": {"$gte": id_min, "$lte": id_max}}),
                    FindOptions::builder().batch_size(10000).build(),
                )
                .and_then(|cursor| {
                    for doc in cursor {
                        buffer.push(doc.unwrap());
                        if buffer.len() == buf_size {
                            let mut data_to_write = Vec::with_capacity(buf_size);
                            std::mem::swap(&mut buffer, &mut data_to_write);
                            target_coll.insert_many(data_to_write, None)?;
                        }
                    }

                    Ok(())
                });

            match res {
                Err(e) => {
                    let _ = sender.send(SyncTableStatus::Failed(SyncError::MongoError(e)));
                }
                Ok(_) => {
                    let _ = sender.send(SyncTableStatus::Done);
                }
            };
        })
    }

    let mut count = 0;
    while let Ok(event) = receiver.recv() {
        match event {
            SyncTableStatus::Failed(e) => return Err(e),
            SyncTableStatus::Done => {
                count += 1;
                if count == doc_concurrent {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn split_ids(coll: &Collection, doc_concurrent: usize) -> Result<Vec<(ObjectId, ObjectId)>> {
    let count = coll.count_documents(None, None)? as usize;

    let mut id_ranges: Vec<(ObjectId, ObjectId)> = Vec::with_capacity(doc_concurrent);
    let docs_per_worker = count / doc_concurrent;
    let null_id_bytes = [0; 12];
    for i in 0..doc_concurrent - 1 {
        // We can assume that the doc is always exists.
        let mut min_id = coll
            .find_one(
                None,
                FindOneOptions::builder()
                    .sort(doc! {"_id": 1})
                    .skip((i * docs_per_worker) as i64)
                    .build(),
            )?
            .unwrap();
        let mut max_id = coll
            .find_one(
                None,
                FindOneOptions::builder()
                    .sort(doc! {"_id": 1})
                    .skip(((i + 1) * docs_per_worker) as i64 - 1)
                    .build(),
            )?
            .unwrap();

        let min_id = min_id.get_object_id_mut("_id").unwrap();
        let min_id = std::mem::replace(min_id, ObjectId::with_bytes(null_id_bytes));
        let max_id = max_id.get_object_id_mut("_id").unwrap();
        let max_id = std::mem::replace(max_id, ObjectId::with_bytes(null_id_bytes));
        id_ranges.push((min_id, max_id));
    }

    // last worker get remain ids.
    let mut last_min_id = coll
        .find_one(
            None,
            FindOneOptions::builder()
                .sort(doc! {"_id": 1})
                .skip(((doc_concurrent - 1) * docs_per_worker) as i64)
                .build(),
        )?
        .unwrap();
    let last_min_id = last_min_id.get_object_id_mut("_id").unwrap();
    let last_min_id = std::mem::replace(last_min_id, ObjectId::with_bytes(null_id_bytes));
    let mut last_max_id = coll
        .find_one(
            None,
            FindOneOptions::builder().sort(doc! {"_id": -1}).build(),
        )?
        .unwrap();
    let last_max_id = last_max_id.get_object_id_mut("_id").unwrap();
    let last_max_id = std::mem::replace(last_max_id, ObjectId::with_bytes(null_id_bytes));
    id_ranges.push((last_min_id, last_max_id));
    Ok(id_ranges)
}

fn sync_one_serial(source_coll: Collection, target_coll: Collection) -> Result<()> {
    let buf_size = 10000;
    let mut buffer = Vec::with_capacity(buf_size);
    let cursor = source_coll
        .find(None, FindOptions::builder().batch_size(10000).build())
        .unwrap();
    for doc in cursor {
        buffer.push(doc.unwrap());
        if buffer.len() == buf_size {
            let mut data_to_write = Vec::with_capacity(buf_size);
            std::mem::swap(&mut buffer, &mut data_to_write);
            target_coll.insert_many(data_to_write, None)?;
        }
    }
    Ok(())
}
