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
//! db = "database"
//! colls = ["a", "b"]
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
    pub fn get_colls(&self) -> &Option<Vec<String>> {
        &self.sync.colls
    }

    /// get concurrent to sync collections.
    pub fn get_collection_concurrent(&self) -> usize {
        self.sync.collection_concurrent
    }

    /// get document concurrent to sync inside one collection.
    pub fn get_doc_concurrent(&self) -> usize {
        self.sync.doc_concurrent
    }

    /// get record collection name.
    ///
    /// The collection is used to record current sync status.
    pub fn get_record_collection(&self) -> &str {
        &self.sync.record_collection
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

/// Logger config, for now it just includes where to save last optime.
#[derive(Deserialize, Debug)]
pub struct Log {
    optime_path: String,
}
