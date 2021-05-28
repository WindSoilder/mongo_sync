mod blocking;
mod config;
mod error;
mod oplog;

const ADMIN_DB_NAME: &str = "admin";
const OPLOG_DB: &str = "local";
const OPLOG_COLL: &str = "oplog.rs";

// local oplog storage database, and collection name
const LOG_STORAGE_DB: &str = "source_oplog";
const LOG_STORAGE_COLL: &str = "source_oplog";

// oplog relative key.
const NAMESPACE_KEY: &str = "ns";
const TIMESTAMP_KEY: &str = "ts";
const OP_KEY: &str = "op";
const NOOP_OP: &str = "n";

pub use blocking::{Connection, MongoSyncer, OplogSyncer};
pub use config::{DbSyncConf, SyncerConfig};
pub use error::{Result, SyncError};
pub use oplog::v2::oplog::Oplog;
