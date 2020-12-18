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
        self.manager.sync_full();

        match self.receiver.recv() {
            Ok(_) => Ok(()),
            Err(e) => Err(SyncError::ReceiveStatusError(e)),
        }
    }

    pub fn sync_incremental(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(self) -> Result<()> {
        self.sync_full();
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
}

impl SyncManager {
    pub fn new(conn: Connection, sender: Sender<ManagerTaskStatus>) -> SyncManager {
        SyncManager {
            conn,
            sender,
            pool: ThreadPoolBuilder::new().num_threads(10).build().unwrap(),
        }
    }

    pub fn sync_full(&self) -> Result<()> {
        // TODO: make decision by configuration.
        let (sender, receiver) = channel::bounded(10);

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
                // TODO: don't use unwrap, should send error through channel.
                target_coll.drop(None).unwrap();

                let mut buffer = vec![];
                let mut cursor = source_coll.find(None, None).unwrap();
                while let Some(doc) = cursor.next() {
                    buffer.push(doc.unwrap());
                    if buffer.len() == 10000 {
                        let mut data_to_write = vec![];
                        std::mem::swap(&mut buffer, &mut data_to_write);
                        target_coll.insert_many(data_to_write, None).unwrap();
                    }
                }
                sender.send(SyncTableStatus::Done);
            })
        }

        while let Ok(_) = receiver.recv() {
            complete_count += 1;
            if total == complete_count {
                let _ = self.sender.send(ManagerTaskStatus::Done);
            }
        }
        Ok(())
    }

    pub fn sync_incr(&self) -> Result<()> {
        unimplemented!()
    }
}
