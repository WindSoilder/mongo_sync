mod blocking;
mod config; // TODO: remove the old config, for easily debug, keep it here for now.
mod config_v2;
mod error;
mod oplog;

const ADMIN_DB_NAME: &str = "admin";
const OPLOG_DB: &str = "local";
const OPLOG_COLL: &str = "oplog.rs";

// oplog relative key.
const NAMESPACE_KEY: &str = "ns";
const TIMESTAMP_KEY: &str = "ts";
const OP_KEY: &str = "op";
const NOOP_OP: &str = "n";

pub use blocking::{Connection, MongoSyncer, OplogSyncer};
pub use config::SyncerConfig; // TODO: remove the old config, for easily debug, keep it here for now.
pub use config_v2::{DbSyncConf, SyncerConfig as SyncerConfigV2};
pub use error::{Result, SyncError};
pub use oplog::v2::oplog::Oplog;
