use crate::error::{Result, SyncError};
use crate::DbSyncConf;
use crate::{ADMIN_DB_NAME, LOG_STORAGE_COLL, LOG_STORAGE_DB};
use bson::Document;
use mongodb::sync::{Client, Collection, Database};

#[derive(Clone)]
/// A simple abstraction for mongodb syncer connection.
pub struct Connection<'a> {
    inner: ConnectionInner<'a>,
}

impl<'a> Connection<'a> {
    /// create a new connection from given `config`.
    pub fn new(config: &DbSyncConf) -> Result<Connection> {
        let source_conn = Client::with_uri_str(config.get_src_uri())?;
        let target_conn = Client::with_uri_str(config.get_dst_uri())?;
        let oplog_storage_conn = Client::with_uri_str(config.get_oplog_storage_uri())?;
        Ok(Connection {
            inner: ConnectionInner {
                source_conn,
                target_conn,
                oplog_storage_conn,
                config,
            },
        })
    }

    /// Check if we have enough permissions to run sync progress.
    pub fn check_permissions(&self) -> Result<()> {
        self.inner.check_permissions()
    }

    /// get database to sync.
    pub fn get_src_db(&self) -> Database {
        self.inner.source_conn.database(self.inner.config.get_db())
    }

    /// get target database to save.
    pub fn get_target_db(&self) -> Database {
        self.inner.target_conn.database(self.inner.config.get_db())
    }

    /// get sync time record collection.
    pub fn time_record_coll(&self) -> Collection<Document> {
        self.get_target_db()
            .collection(self.inner.config.get_record_collection())
    }

    /// get target mongodb client.
    pub fn get_target_client(&self) -> Client {
        self.inner.target_conn.clone()
    }

    /// return collection which saves oplog.
    pub fn oplog_coll(&self) -> Collection<Document> {
        self.inner
            .oplog_storage_conn
            .database(LOG_STORAGE_DB)
            .collection(LOG_STORAGE_COLL)
    }

    /// return colleciton which saves extra admin infor.
    pub fn get_target_admin_db(&self) -> Database {
        self.inner.target_conn.database(ADMIN_DB_NAME)
    }

    /// get sync configuration.
    pub fn get_conf(&self) -> &DbSyncConf {
        self.inner.config
    }
}

#[derive(Clone)]
struct ConnectionInner<'a> {
    source_conn: Client,
    target_conn: Client,
    oplog_storage_conn: Client,
    config: &'a DbSyncConf,
}

impl<'a> ConnectionInner<'a> {
    pub fn check_permissions(&self) -> Result<()> {
        // TODO: add admin access check, to ensure that we can execute applylog command.
        let db_name = self.config.get_db();
        let source_db = self.source_conn.database(db_name);
        if let Err(e) = source_db.list_collection_names(None) {
            return Err(SyncError::PermissionError {
                uri: self.config.get_src_uri().to_string(),
                db: db_name.to_string(),
                detail: e,
            });
        }

        let target_db = self.target_conn.database(db_name);
        if let Err(e) = target_db.list_collection_names(None) {
            return Err(SyncError::PermissionError {
                uri: self.config.get_dst_uri().to_string(),
                db: db_name.to_string(),
                detail: e,
            });
        }

        Ok(())
    }
}
