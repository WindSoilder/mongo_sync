use crate::config::SyncerConfig;
use crate::error::{Result, SyncError};
use mongodb::sync::{Client, Database};
use std::sync::Arc;

#[derive(Clone)]
pub struct Connection {
    inner: ConnectionInner,
}

impl Connection {
    pub fn new(config: Arc<SyncerConfig>) -> Result<Connection> {
        let source_conn = Client::with_uri_str(config.get_src_url())?;
        let target_conn = Client::with_uri_str(config.get_dst_url())?;
        Ok(Connection {
            inner: ConnectionInner {
                source_conn,
                target_conn,
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

    pub fn get_conf(&self) -> Arc<SyncerConfig> {
        self.inner.config.clone()
    }
}

#[derive(Clone)]
struct ConnectionInner {
    source_conn: Client,
    target_conn: Client,
    config: Arc<SyncerConfig>,
}

impl ConnectionInner {
    pub fn check_permissions(&self) -> Result<()> {
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
