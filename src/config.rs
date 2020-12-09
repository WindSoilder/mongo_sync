//! mongo_syncer basic configuration, express in toml.
//!
//! Basic configuration file example:
//! ```toml
//! [src]
//! url = "mongodb://localhost/abc"
//!
//! [dst]
//! url = "mognodb://localhost/def"
//!
//! [sync]
//! dbs = [
//!     { db = "database2" }
//! ]
//!
//! ```
use serde::Deserialize;

/// Global mongo syncer configuration.
#[derive(Deserialize, Debug)]
pub struct SyncerConfig {
    src: Src,
    dst: Dst,
    sync: DetailSyncConf,
}

impl SyncerConfig {
    /// get source mongodb url.
    pub fn get_src_url(&self) -> &str {
        &self.src.url
    }

    /// get destination mongodb url.
    pub fn get_dst_url(&self) -> &str {
        &self.dst.url
    }

    /// get database to sync.
    pub fn get_db(&self) -> &str {
        &self.sync.db
    }

    /// get collections to sync.
    pub fn get_colls(&self) -> Option<&Vec<String>> {
        self.sync.colls.as_ref()
    }
}

/// Source database confuration.
#[derive(Deserialize, Debug)]
pub struct Src {
    /// Source database url, it needs to be replica set, begins with 'mongodb://'
    url: String,
}

/// Target database configuration.
#[derive(Deserialize, Debug)]
pub struct Dst {
    /// Target database url.  Which begins with 'mongodb://'
    url: String,
}

/// Detail sync config, it indicates which database to sync, or which collection to sync.
#[derive(Deserialize, Debug)]
pub struct DetailSyncConf {
    /// database name
    db: String,
    /// collections to sync, default it None, which means sync all collections.
    #[serde(default = "default_collections")]
    colls: Option<Vec<String>>,
}

fn default_collections() -> Option<Vec<String>> {
    None
}

/// Logger config, for now it just includes where to save last optime.
#[derive(Deserialize, Debug)]
pub struct Log {
    optime_path: String,
}
