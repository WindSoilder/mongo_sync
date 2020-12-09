use crate::config::SyncerConfig;
use crate::{Result, SyncError};
use mongodb::{Client, Database};

pub struct DbConnection<'a> {
    source: Client,
    target: Client,
    db_name: &'a str,
    colls: Option<&'a Vec<String>>,
    source_uri: &'a str,
    target_uri: &'a str,
}

impl DbConnection<'_> {
    pub async fn connect(conf: &SyncerConfig) -> Result<DbConnection<'_>> {
        // try to connect to source, target mongodb.
        let source = Client::with_uri_str(conf.get_src_url()).await?;
        let target = Client::with_uri_str(conf.get_dst_url()).await?;

        // initialize relative dbs, which indicates databases to sync.
        Ok(DbConnection {
            source,
            target,
            db_name: conf.get_db(),
            colls: conf.get_colls(),
            source_uri: conf.get_src_url(),
            target_uri: conf.get_dst_url(),
        })
    }

    pub async fn check_permission(&self) -> Result<()> {
        if let Err(e) = self
            .source
            .database(&self.db_name)
            .list_collection_names(None)
            .await
        {
            return Err(SyncError::PermissionError {
                uri: self.source_uri.to_string(),
                db: self.db_name.to_string(),
                detail: e,
            });
        }
        if let Err(e) = self
            .target
            .database(&self.db_name)
            .list_collection_names(None)
            .await
        {
            return Err(SyncError::PermissionError {
                uri: self.target_uri.to_string(),
                db: self.db_name.to_string(),
                detail: e,
            });
        }
        Ok(())
    }

    pub fn get_source_client(&self) -> &Client {
        &self.source
    }

    pub fn get_target_client(&self) -> &Client {
        &self.target
    }

    pub fn get_source_database(&self) -> Database {
        self.source.database(&self.db_name)
    }

    pub fn get_target_database(&self) -> Database {
        self.target.database(&self.db_name)
    }
}
