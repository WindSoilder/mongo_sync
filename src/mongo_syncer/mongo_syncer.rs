use crate::DbConnection;
use crate::Result;

pub struct MongoSyncer<'a> {
    db_conn: DbConnection<'a>,
}

impl MongoSyncer<'_> {
    pub fn new(db_conn: DbConnection<'_>) -> MongoSyncer<'_> {
        MongoSyncer { db_conn }
    }

    pub fn main(self) -> Result<()> {
        Ok(())
    }
}
