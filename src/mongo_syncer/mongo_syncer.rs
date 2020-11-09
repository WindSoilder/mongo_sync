use crate::DbConnection;
use crate::Result;
use crate::Oplog;

pub struct MongoSyncer<'a> {
    db_conn: DbConnection<'a>,
}

impl MongoSyncer<'_> {
    pub fn new(db_conn: DbConnection<'_>) -> MongoSyncer<'_> {
        MongoSyncer { db_conn }
    }

    pub async fn main(self) -> Result<()> {
        loop {
            let source = self.db_conn.get_source_client();
            // read from oplog.
            let a = source
                .database("local")
                .collection("oplog.rs")
                .find_one(None, None)
                .await
                .unwrap();

            let oplog = Oplog::from_doc(a);
            if oplog.is_valid(self.db_conn.get_databases()) {
                oplog.apply(self.db_conn.get_target_client()).await;
            }
        }
    }
}
