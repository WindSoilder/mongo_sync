#![allow(missing_docs)]

use bson::document::ValueAccessError;
use bson::Document;
use crossbeam::channel::RecvError;
use mongodb::error::Error as MongoError;
use std::backtrace::Backtrace;
use std::result::Result as StdResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Mongodb connection error")]
    MongoError {
        #[from]
        source: MongoError,
        backtrace: Backtrace,
    },
    #[error("Check permission for database {db:?} failed, connection string: {uri:?}, detailed: {detail:?}")]
    PermissionError {
        uri: String,
        db: String,
        detail: MongoError,
    },
    #[error("Mongodb document value error")]
    BsonError {
        #[from]
        source: ValueAccessError,
        backtrace: Backtrace,
    },
    #[error("Invalid doc value for bson, get key: {key:?}, val: {val:?}")]
    BsonValueError { key: String, val: String },
    #[error("Receiver task message error")]
    ReceiveStatusError {
        #[from]
        source: RecvError,
        backtrace: Backtrace,
    },
    #[error("Can't fetch doc from mongodb")]
    EmptyDocError,
    #[error("apply oplogs error")]
    ApplyOplogError(Document),
}

pub type Result<T> = StdResult<T, SyncError>;
