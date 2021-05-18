mod blocking;
mod config; // TODO: remove the old config, for easily debug, keep it here for now.
mod config_v2;
mod error;
mod oplog;

pub use blocking::{Connection, MongoSyncer};
pub use config::SyncerConfig; // TODO: remove the old config, for easily debug, keep it here for now.
pub use config::SyncerConfig as SyncerConfigV2;
pub use error::{Result, SyncError};
pub use oplog::v2::oplog::Oplog;

#[macro_use]
extern crate log;
