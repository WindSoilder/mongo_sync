use crate::config::SyncerConfig;
use crate::{Result, SyncError};
use mongodb::Client;

pub struct DbConnection<'a> {
    source: Client,
    target: Client,
    dbs: Vec<String>,
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
            dbs: conf
                .get_db_sync_info()
                .iter()
                .map(|db_conf| db_conf.get_name().to_string())
                .collect(),
            source_uri: conf.get_src_url(),
            target_uri: conf.get_dst_url(),
        })
    }

    pub async fn check_permission(&self) -> Result<()> {
        for db_name in self.dbs.iter() {
            if let Err(e) = self
                .source
                .database(db_name)
                .list_collection_names(None)
                .await
            {
                return Err(SyncError::PermissionError {
                    uri: self.source_uri.to_string(),
                    db: db_name.clone(),
                    detail: e,
                });
            }
            if let Err(e) = self
                .target
                .database(db_name)
                .list_collection_names(None)
                .await
            {
                return Err(SyncError::PermissionError {
                    uri: self.target_uri.to_string(),
                    db: db_name.clone(),
                    detail: e,
                });
            }
        }
        Ok(())
    }

    pub fn get_source_client(&self) -> &Client {
        &self.source
    }

    pub fn get_target_client(&self) -> &Client {
        &self.target
    }

    pub fn get_databases(&self) -> &[String] {
        &self.dbs
    }

    pub fn get_source_uri(&self) -> &str {
        self.source_uri
    }

    pub fn get_target_uri(&self) -> &str {
        self.target_uri
    }
}
