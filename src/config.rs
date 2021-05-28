//! mongo_syncer basic configuration, express in toml.
//!
//! Basic configuration file example:
//!
//! [src]
//! # source db url, need to be a replica set.
//! url = "mongodb://root:Ricemap123@192.168.10.67/?authSource=admin"
//!
//! [oplog_storage]
//! uri = "mongodb://localhost:27018/"
//!
//! [[sync]]
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
    oplog_storage: OplogStorage,
    sync: Vec<DetailSyncConf>,
}

impl SyncerConfig {
    /// get source mongodb url.
    pub fn get_src_url(&self) -> &str {
        &self.src.url
    }

    pub fn get_oplog_storage_uri(&self) -> &str {
        &self.oplog_storage.uri
    }

    pub fn get_detail_sync_conf(&self) -> &Vec<DetailSyncConf> {
        &self.sync
    }
}

#[derive(Deserialize, Debug)]
pub struct OplogStorage {
    /// Oplog storage uri
    uri: String,
}

/// Source database configuration.
#[derive(Deserialize, Debug)]
pub struct Src {
    /// Source database url, it needs to be replica set, begins with 'mongodb://'
    url: String,
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

#[derive(Debug)]
pub struct DbSyncConf {
    src: Src,
    oplog_storage: OplogStorage,
    conf: DetailSyncConf,
}

impl DbSyncConf {
    pub fn new(
        src_uri: String,
        target_uri: String,
        oplog_storage_uri: String,
        db: String,
        colls: Option<Vec<String>>,
        collection_concurrent: Option<usize>,
        doc_concurrent: Option<usize>,
        record_collection: Option<String>,
    ) -> Self {
        DbSyncConf {
            src: Src { url: src_uri },
            oplog_storage: {
                OplogStorage {
                    uri: oplog_storage_uri,
                }
            },
            conf: DetailSyncConf {
                dst_url: target_uri,
                db,
                colls,
                collection_concurrent: collection_concurrent.unwrap_or_else(number_of_cpus),
                doc_concurrent: doc_concurrent.unwrap_or_else(half_number_of_cpus),
                record_collection: record_collection.unwrap_or_else(default_record_collection),
            },
        }
    }

    pub fn get_db(&self) -> &str {
        &self.conf.db
    }

    pub fn get_record_collection(&self) -> &str {
        "oplog_records"
    }

    pub fn get_oplog_storage_url(&self) -> &str {
        &self.oplog_storage.uri
    }

    pub fn get_dst_url(&self) -> &str {
        &self.conf.dst_url
    }

    pub fn get_src_url(&self) -> &str {
        &self.src.url
    }

    pub fn get_collection_concurrent(&self) -> usize {
        self.conf.collection_concurrent
    }

    pub fn get_doc_concurrent(&self) -> usize {
        self.conf.doc_concurrent
    }

    pub fn get_colls(&self) -> &Option<Vec<String>> {
        &self.conf.colls
    }
}
