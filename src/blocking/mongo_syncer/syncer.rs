use crate::blocking::connection::Connection;
use crate::error::{Result, SyncError};
use crossbeam::channel::{self, Receiver, Sender};
use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct MongoSyncer {
    manager: SyncManager,
    receiver: Receiver<ManagerTaskStatus>,
}

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
    sender: Sender<ManagerTaskStatus>,
}

enum SyncTableStatus {
    Done,
    Abort(SyncError),
    Failed(SyncError),
}

impl SyncManager {
    pub fn new(conn: Connection, sender: Sender<ManagerTaskStatus>) -> SyncManager {
        let coll_concurrent = conn.get_conf().get_collection_concurrent();
        SyncManager {
            conn,
            sender,
            pool: ThreadPoolBuilder::new()
                .num_threads(coll_concurrent)
                .build()
                .unwrap(),
        }
    }

    pub fn sync_full(&self) -> Result<()> {
        let coll_concurrent = self.conn.get_conf().get_collection_concurrent();
        let (sender, receiver) = channel::bounded(coll_concurrent);

        let mut complete_count = 0;
        info!("{:?}", self.conn.get_conf().get_colls());
        let coll_names = match self.conn.get_conf().get_colls() {
            // use unwrap here is ok, because we have check list_collection_names before.
            None => self.conn.get_src_db().list_collection_names(None).unwrap(),
            Some(colls) => colls.clone(),
        };
        let total = coll_names.len() as u32;
        info!("{:?}", coll_names);
        for coll in coll_names.into_iter() {
            let sender = sender.clone();
            let source_coll = self.conn.get_src_db().collection(&coll);
            let target_coll = self.conn.get_target_db().collection(&coll);

            self.pool.spawn(move || {
                if let Err(e) = target_coll.drop(None) {
                    error!("Drop target collection failed, error msg: {:?}", e);
                    let _ = sender.send(SyncTableStatus::Failed(SyncError::MongoError(e)));
                    return;
                }

                let buf_size = 10000;
                let mut buffer = Vec::with_capacity(buf_size);
                let cursor = source_coll.find(None, None).unwrap();
                for doc in cursor {
                    buffer.push(doc.unwrap());
                    if buffer.len() == buf_size {
                        let mut data_to_write = Vec::with_capacity(buf_size);
                        std::mem::swap(&mut buffer, &mut data_to_write);

                        if let Err(e) = target_coll.drop(None) {
                            error!("Insert data to target collection failed, error msg: {:?}", e);
                            let _ = sender.send(SyncTableStatus::Failed(SyncError::MongoError(e)));
                            return;
                        }
                    }
                }

                // TODO: sync index.
                let _ = sender.send(SyncTableStatus::Done);
            })
        }

        while let Ok(_) = receiver.recv() {
            complete_count += 1;
            if total == complete_count {
                let _ = self.sender.send(ManagerTaskStatus::Done);
                break;
            }
        }
        Ok(())
    }

    pub fn sync_incr(&self) -> Result<()> {
        unimplemented!()
    }
}
