//! Mongo sync lib, which provides an easily usage api to sync data from one mongodb to another mongodb.
//!
//! Provides two syncer: [MongoSyncer] and [OplogSyncer], and one cleaner [OplogCleaner].
//!
//! [OplogSyncer] is used to sync cluster oplog, and [MongoSyncer] is used to sync mongodb data.
//! [OplogCleaner] is used to clean too old oplog data.
//!
//! # OplogSyncer example:
//! ```no_run
//! use mongo_sync::OplogSyncer;
//! let oplog_syncer = OplogSyncer::new("mongodb://localhost:27017", "mongodb://localhost:27018").unwrap();
//! oplog_syncer.sync_forever();
//! ```
//!
//! # MongoSyncer example:
//! ```no_run
//! use mongo_sync::{DbSyncConf, MongoSyncer};
//!
//! let conf = DbSyncConf::new("mongodb://localhost:27017".to_string(), "mongodb://localhost:27018".to_string(), "mongodb://localhost:27017".to_string(), "a".to_string(), None, None, None);
//! let syncer = MongoSyncer::new(&conf);
//! syncer.sync();
//! ```
//!
//! # OplogCleaner example:
//! ```no_run
//! use mongo_sync::OplogCleaner;
//! let oplog_cleaner = OplogCleaner::new("mongodb://localhost:27018".to_string());
//! oplog_cleaner.run_clean();
//! ```

#![warn(missing_docs)]
#![feature(backtrace)]

#[doc(hidden)]
pub mod blocking;
pub mod cmd_oplog;
mod config;
mod error;

/// mongodb internal database for admin.
const ADMIN_DB_NAME: &str = "admin";
/// mongodb internal database which saves oplogs.
const OPLOG_DB: &str = "local";
/// mongodb internal collection which saves oplogs.
const OPLOG_COLL: &str = "oplog.rs";

/// local oplog storage database name.
const LOG_STORAGE_DB: &str = "source_oplog";
/// local oplog storage collection name.
const LOG_STORAGE_COLL: &str = "source_oplog";

/// oplog namespace key name.
const NAMESPACE_KEY: &str = "ns";
/// oplog timestamp key name.
const TIMESTAMP_KEY: &str = "ts";
/// oplog operation key name.
const OP_KEY: &str = "op";
/// noop operation.
const NOOP_OP: &str = "n";
/// command operation.
const COMMAND_OP: &str = "c";

pub use blocking::{Connection, MongoSyncer, OplogCleaner, OplogSyncer};
pub use config::{DbSyncConf, OplogSyncerConfig};
pub use error::{Result, SyncError};
