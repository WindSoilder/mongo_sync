use bson::document::ValueAccessError;
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
}

pub type Result<T> = StdResult<T, SyncError>;
