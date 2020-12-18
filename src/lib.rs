mod config;
mod error;
mod blocking;
mod oplog;

pub use config::SyncerConfig;
pub use error::{Result, SyncError};
pub use oplog::v2::oplog::Oplog;
pub use blocking::{Connection, MongoSyncer};

#[macro_use]
extern crate log;
