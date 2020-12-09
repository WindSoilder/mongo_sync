use crate::oplog::apply;
use crate::DbConnection;
use crate::Oplog;
use crate::Result;
use bson::Timestamp;
use futures::StreamExt;
use mongodb::Cursor;

pub struct MongoSyncer<'a> {
    db_conn: DbConnection<'a>,
}

impl MongoSyncer<'_> {
    pub fn new(db_conn: DbConnection<'_>) -> MongoSyncer<'_> {
        MongoSyncer { db_conn }
    }

    pub async fn main(self) -> Result<()> {
        loop {
            let latest = self.get_latest_update_time().await;

            // read from oplog.
            let mut cursor = self.get_logs_cursor(latest).await?;
            while let Some(doc_result) = cursor.next().await {

            }
            break Ok(());
        }
    }

    pub async fn sync_full(&mut self) -> Result<()> {
        // get collections to sync.
        // sync every collection one by one.
        Ok(())
    }

    async fn get_latest_update_time(&self) -> Timestamp {
        unimplemented!()
    }

    async fn get_logs_cursor(&self, from_time: Timestamp) -> Result<Cursor> {
        unimplemented!()
    }
}
