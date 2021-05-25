use crate::config_v2::DbSyncConf;
use crate::error::{Result, SyncError};
use crate::{ADMIN_DB_NAME, LOG_STORAGE_COLL, LOG_STORAGE_DB, OPLOG_COLL, OPLOG_DB};
use mongodb::sync::{Client, Collection, Database};
use std::sync::Arc;

#[derive(Clone)]
pub struct Connection {
    inner: ConnectionInner,
}

impl Connection {
    pub fn new(config: Arc<DbSyncConf>) -> Result<Connection> {
        let source_conn = Client::with_uri_str(config.get_src_url())?;
        let target_conn = Client::with_uri_str(config.get_dst_url())?;
        let oplog_storage_conn = Client::with_uri_str(config.get_oplog_storage_url())?;
        Ok(Connection {
            inner: ConnectionInner {
                source_conn,
                target_conn,
                oplog_storage_conn,
                config,
            },
        })
    }

    pub fn check_permissions(&self) -> Result<()> {
        self.inner.check_permissions()
    }

    pub fn get_src_db(&self) -> Database {
        self.inner.source_conn.database(self.inner.config.get_db())
    }

    pub fn get_target_db(&self) -> Database {
        self.inner.target_conn.database(self.inner.config.get_db())
    }

    pub fn time_record_coll(&self) -> Collection {
        self.get_target_db()
            .collection(self.inner.config.get_record_collection())
    }

    pub fn oplog_coll(&self) -> Collection {
        self.inner
            .oplog_storage_conn
            .database(LOG_STORAGE_DB)
            .collection(LOG_STORAGE_COLL)
    }

    pub fn get_target_admin_db(&self) -> Database {
        self.inner.target_conn.database(ADMIN_DB_NAME)
    }

    pub fn get_conf(&self) -> Arc<DbSyncConf> {
        self.inner.config.clone()
    }
}

#[derive(Clone)]
struct ConnectionInner {
    source_conn: Client,
    target_conn: Client,
    oplog_storage_conn: Client,
    config: Arc<DbSyncConf>,
}

impl ConnectionInner {
    pub fn check_permissions(&self) -> Result<()> {
        // TODO: add admin access check, to ensure that we can execute applylog command.
        let db_name = self.config.get_db();
        let source_db = self.source_conn.database(db_name);
        if let Err(e) = source_db.list_collection_names(None) {
            return Err(SyncError::PermissionError {
                uri: self.config.get_src_url().to_string(),
                db: db_name.to_string(),
                detail: e,
            });
        }

        let target_db = self.target_conn.database(db_name);
        if let Err(e) = target_db.list_collection_names(None) {
            return Err(SyncError::PermissionError {
                uri: self.config.get_dst_url().to_string(),
                db: db_name.to_string(),
                detail: e,
            });
        }

        Ok(())
    }
}
