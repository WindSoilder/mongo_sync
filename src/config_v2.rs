//! mongo_syncer basic configuration, express in toml.
//!
//! Basic configuration file example:
//! ```toml
//! [src]
//! # source db url, need to be a replica set.
//! url = "mongodb://rice:Ricemap123@localhost/?authSource=admin"
//!
//! [oplog]
//! # mongodb url to sync oplog from source client.
//! storage_url = "mongodb://root:Ricemap123@192.168.10.67/?authSource=admin"
//!
//! [[sync]]
//! # source db url, need to be a replica set.
//! src_url = "mongodb://rice:Ricemap123@localhost/?authSource=admin"
//! # target db url, don't need to be a replica set.
//! dst_url = "mongodb://root:Ricemap123@192.168.10.67/?authSource=admin"
//! # specify database to sync.
//! db = "bb"
//! colls = ["a", "b"]
//! ```
use serde::Deserialize;

/// Global mongo syncer configuration.
#[derive(Deserialize, Debug)]
pub struct SyncerConfig {
    src: Src,
    oplog: Oplog,
    sync: Vec<DetailSyncConf>,
}

impl SyncerConfig {
    /// get source mongodb url.
    pub fn get_src_url(&self) -> &str {
        &self.src.url
    }

    /// get oplog storage url, oplog on source database will be synced to here.
    pub fn get_oplog_storage_url(&self) -> &str {
        &self.oplog.storage_url
    }

    pub fn get_detail_sync_conf(&self) -> &Vec<DetailSyncConf> {
        &self.sync
    }
}

/// Source database configuration.
#[derive(Deserialize, Debug)]
pub struct Src {
    /// Source database url, it needs to be replica set, begins with 'mongodb://'
    url: String,
}

/// Oplog sync configuration.
#[derive(Deserialize, Debug)]
pub struct Oplog {
    /// mongodb url to store oplog from source database.
    storage_url: String,
}

/// Detail sync config, it indicates which database to sync, or which collection to sync.
#[derive(Deserialize, Debug)]
pub struct DetailSyncConf {
    /// target db url.
    dst_url: String,
    /// database name
    db: String,
    /// collections to sync, default it None, which means sync all collections.
    #[serde(default = "default_collections")]
    colls: Option<Vec<String>>,
    /// how many collections will be sync concurrently.
    #[serde(default = "number_of_cpus")]
    collection_concurrent: usize,
    /// how many threads will used to sync one collection concurrently.
    #[serde(default = "half_number_of_cpus")]
    doc_concurrent: usize,
    /// which time record collection will be written to.
    #[serde(default = "default_record_collection")]
    record_collection: String,
}

fn default_record_collection() -> String {
    "sync_time_record".to_string()
}

fn default_collections() -> Option<Vec<String>> {
    None
}

fn number_of_cpus() -> usize {
    num_cpus::get()
}

fn half_number_of_cpus() -> usize {
    num_cpus::get() / 2
}
