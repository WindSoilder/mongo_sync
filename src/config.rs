/// Global mongo syncer configuration.
#[derive(Debug)]
pub struct OplogSyncerConfig {
    src: Src,
    oplog_storage: OplogStorage,
}

impl OplogSyncerConfig {
    /// get source mongodb uri.
    pub fn get_src_uri(&self) -> &str {
        &self.src.uri
    }

    /// get target oplog storage mongodb uri.
    pub fn get_oplog_storage_uri(&self) -> &str {
        &self.oplog_storage.uri
    }
}

#[derive(Debug)]
pub struct OplogStorage {
    /// Oplog storage uri
    uri: String,
}

/// Source database configuration.
#[derive(Debug)]
pub struct Src {
    /// Source database uri, it needs to be replica set, begins with 'mongodb://'
    uri: String,
}

/// Detail sync config, it indicates which database to sync, or which collection to sync.
#[derive(Debug)]
pub struct DetailSyncConf {
    /// target db uri.
    dst_uri: String,
    /// database name
    db: String,
    /// collections to sync, default it None, which means sync all collections.
    colls: Option<Vec<String>>,
    /// how many collections will be sync concurrently.
    collection_concurrent: usize,
    /// how many threads will used to sync one collection concurrently.
    doc_concurrent: usize,
}

fn number_of_cpus() -> usize {
    num_cpus::get()
}

fn half_number_of_cpus() -> usize {
    num_cpus::get() / 2
}

#[derive(Debug)]
/// database sync configuration.
pub struct DbSyncConf {
    src: Src,
    oplog_storage: OplogStorage,
    conf: DetailSyncConf,
}

impl DbSyncConf {
    /// create a new configuration.
    pub fn new(
        src_uri: String,
        target_uri: String,
        oplog_storage_uri: String,
        db: String,
        colls: Option<Vec<String>>,
        collection_concurrent: Option<usize>,
        doc_concurrent: Option<usize>,
    ) -> Self {
        DbSyncConf {
            src: Src { uri: src_uri },
            oplog_storage: {
                OplogStorage {
                    uri: oplog_storage_uri,
                }
            },
            conf: DetailSyncConf {
                dst_uri: target_uri,
                db,
                colls,
                collection_concurrent: collection_concurrent.unwrap_or_else(number_of_cpus),
                doc_concurrent: doc_concurrent.unwrap_or_else(half_number_of_cpus),
            },
        }
    }

    /// get database to sync.
    pub fn get_db(&self) -> &str {
        &self.conf.db
    }

    /// get sync record collection name.
    pub fn get_record_collection(&self) -> &str {
        "oplog_records"
    }

    /// get oplog storage uri, which will save oplogs from source cluster.
    pub fn get_oplog_storage_uri(&self) -> &str {
        &self.oplog_storage.uri
    }

    /// get destination database uri.
    pub fn get_dst_uri(&self) -> &str {
        &self.conf.dst_uri
    }

    /// get source database uri.
    pub fn get_src_uri(&self) -> &str {
        &self.src.uri
    }

    /// return how many threads will be used when sync collecitons.
    pub fn get_collection_concurrent(&self) -> usize {
        self.conf.collection_concurrent
    }

    /// return how many threads will be used when sync one collection.
    pub fn get_doc_concurrent(&self) -> usize {
        self.conf.doc_concurrent
    }

    /// get collections to sync.
    ///
    /// When return None, it indicates that sync all collections in a database.
    pub fn get_colls(&self) -> &Option<Vec<String>> {
        &self.conf.colls
    }
}
