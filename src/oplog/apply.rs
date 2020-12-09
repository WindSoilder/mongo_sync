//! apply oplog to mongodb.
use super::v2::oplog::Oplog;
use crate::Result;
use mongodb::Client;

pub async fn apply(client: Client, oplog: Oplog) -> Result<()> {
    unimplemented!()
}

pub async fn apply_vec(client: Client, oplog: Oplog) -> Result<()> {
    unimplemented!()
}
