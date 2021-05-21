use bson::document::ValueAccessError;
use crossbeam::channel::RecvError;
use mongodb::error::Error as MongoError;
use std::result::Result as StdResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Mongodb connection error")]
    MongoError(#[from] MongoError),
    #[error("Check permission for database {db:?} failed, connection string: {uri:?}, detailed: {detail:?}")]
    PermissionError {
        uri: String,
        db: String,
        detail: MongoError,
    },
    #[error("Mongodb document value error")]
    BsonError(#[from] ValueAccessError),
    #[error("Invalid doc value for bson, get key: {key:?}, val: {val:?}")]
    BsonValueError { key: String, val: String },
    #[error("Receiver task message error")]
    ReceiveStatusError(#[from] RecvError),
    #[error("Can't fetch doc from mongodb, url: {url:?}, namespace: {db:?}.{coll:?}")]
    EmptyDocError {
        url: String,
        db: String,
        coll: String,
    },
}

pub type Result<T> = StdResult<T, SyncError>;
