use super::oplog_helper;
use crate::{
    Result, SyncError, LOG_STORAGE_COLL, LOG_STORAGE_DB, NAMESPACE_KEY, NOOP_OP, OPLOG_COLL,
    OPLOG_DB, OP_KEY, TIMESTAMP_KEY,
};
use bson::doc;
use chrono::{Duration, TimeZone, Utc};
use mongodb::bson::{Document, Timestamp};
use mongodb::options::{CursorType, FindOptions};
use mongodb::sync::{Client, Collection, Database};
use std::time::SystemTime;
use tracing::info;

#[derive(Debug)]
/// A syncer to sync oplogs from one mongodb cluster to another mongodb.
pub struct OplogSyncer {
    source_conn: Client,
    storage_conn: Client,
}

const TRUNCATE_POINT_COLL: &str = "oplog_truncate_after_point";

impl OplogSyncer {
    /// Create a new oplog syncer.
    ///
    /// When invoke [`sync_forever`](OplogSyncer::sync_forever), it will sync oplog from mongodb on `src_uri` to
    /// target mongodb uri on `storage_uri`.
    ///
    /// Note that the source mongodb must be a cluster (so the oplog exists), but target mongodb doesn't need to.
    pub fn new(src_uri: &str, storage_uri: &str) -> Result<OplogSyncer> {
        let source_conn = Client::with_uri_str(src_uri)?;
        let storage_conn = Client::with_uri_str(storage_uri)?;

        Ok(OplogSyncer {
            source_conn,
            storage_conn,
        })
    }

    /// Start the syncer, it will run forever to sync oplogs.
    pub fn sync_forever(self) -> Result<()> {
        let storage_latest_ts_may_exists = self.get_storage_latest_ts()?;
        let source_oplog_earliest = oplog_helper::get_earliest_ts(
            &self
                .source_conn
                .database(OPLOG_DB)
                .collection::<Document>(OPLOG_COLL),
        )?;

        let log_storage_coll = self.get_log_storage_coll();
        let truncate_point_coll = self.get_truncate_point_coll();
        // it means that there are some oplogs missing, we can never fetch oplogs between `storage_latest_ts` and `source_oplog_earliest`
        if storage_latest_ts_may_exists.is_none()
            || storage_latest_ts_may_exists.unwrap() < source_oplog_earliest
        {
            info!("Some oplog missing! Begin to re-initialize our local storage database");
            // Initialize our database, to make sure that everything clean.
            log_storage_coll.drop(None)?;
            truncate_point_coll.drop(None)?;

            self.get_log_storage_db().run_command(
                doc! {
                    "createIndexes": LOG_STORAGE_COLL,
                    "indexes": [
                        {
                            "key": { TIMESTAMP_KEY: 1 },
                            "name": format!("{}_1", TIMESTAMP_KEY),
                        },
                    ]
                },
                None,
            )?;
        }
        self.sync_incr_forever()
    }

    fn sync_incr_forever(self) -> Result<()> {
        let truncate_point_coll = self.get_truncate_point_coll();
        let log_storage_coll = self.get_log_storage_coll();

        let truncate_ts = truncate_point_coll
            .find_one(None, None)?
            .map(|d| d.get_timestamp(TIMESTAMP_KEY).unwrap());

        let source_coll = self.get_source_oplog_coll();
        let start_point = match truncate_ts {
            None => oplog_helper::get_latest_ts(&source_coll)?,
            Some(t) => {
                info!(?truncate_ts, "Truncate oplog after given point. ");
                log_storage_coll.delete_many(doc! {TIMESTAMP_KEY: {"$gte": t}}, None)?;
                t
            }
        };

        // `start_time` just use for log...
        const BATCH_DELAY: u64 = 3; // save data every 3 seconds.
        info!(?start_point, "Begin to sync oplog. ");
        // fetch and sync oplog.
        let cursor = source_coll.find(
            doc! {TIMESTAMP_KEY: {"$gte": start_point}},
            FindOptions::builder()
                .cursor_type(CursorType::TailableAwait)
                .build(),
        )?;
        info!(?start_point, "Initial fetch oplog complete. ");
        let mut now = SystemTime::now();
        let mut oplog_batched: Vec<Document> = vec![];
        const BATCH_SIZE: usize = 10000;
        for doc in cursor {
            let doc = doc?;

            if !self.is_useless_oplog(&doc)? {
                oplog_batched.push(doc);
            }

            if (oplog_batched.len() > BATCH_SIZE)
                || (now.elapsed().unwrap().as_secs() >= BATCH_DELAY && !oplog_batched.is_empty())
            {
                let earliest_ts = oplog_batched[0].get_timestamp(TIMESTAMP_KEY)?;
                let latest_ts =
                    oplog_batched[oplog_batched.len() - 1].get_timestamp(TIMESTAMP_KEY)?;

                let mut data_to_write: Vec<Document> = Vec::with_capacity(oplog_batched.len());
                data_to_write.append(&mut oplog_batched);
                info!(
                    "begin to insert oplogs, oplog length: {}",
                    data_to_write.len()
                );
                log_storage_coll.insert_many(data_to_write, None)?;

                info!(?earliest_ts, ?latest_ts, "Sync oplog complete. ");
                self.save_latest_ts(&truncate_point_coll, latest_ts)?;
                info!(
                    ?earliest_ts,
                    ?latest_ts,
                    "Write truncate after point complete. "
                );
                now = SystemTime::now();
            }
        }
        Ok(())
    }

    fn save_latest_ts(
        &self,
        oplog_truncate_after_point: &Collection<Document>,
        latest: Timestamp,
    ) -> Result<()> {
        oplog_truncate_after_point.delete_many(Document::new(), None)?;
        oplog_truncate_after_point.insert_one(
            doc! {
                "ts": latest
            },
            None,
        )?;
        Ok(())
    }

    fn is_useless_oplog(&self, doc: &Document) -> Result<bool> {
        let op = doc.get_str(OP_KEY)?;
        let ns = doc.get_str(NAMESPACE_KEY)?;
        Ok(op == NOOP_OP
            || (ns.starts_with("admin.")
                || ns.starts_with("local.")
                || ns.starts_with("config.")
                || (ns.starts_with(LOG_STORAGE_DB))))
    }

    fn get_log_storage_coll(&self) -> Collection<Document> {
        self.get_log_storage_db()
            .collection::<Document>(LOG_STORAGE_COLL)
    }

    fn get_truncate_point_coll(&self) -> Collection<Document> {
        self.get_log_storage_db()
            .collection::<Document>(TRUNCATE_POINT_COLL)
    }

    fn get_log_storage_db(&self) -> Database {
        self.storage_conn.database(LOG_STORAGE_DB)
    }

    fn get_source_oplog_coll(&self) -> Collection<Document> {
        self.source_conn
            .database(OPLOG_DB)
            .collection::<Document>(OPLOG_COLL)
    }

    fn get_storage_latest_ts(&self) -> Result<Option<Timestamp>> {
        Ok(self
            .get_truncate_point_coll()
            .find_one(None, None)?
            .map(|d| d.get_timestamp(TIMESTAMP_KEY).unwrap()))
    }
}

/// A cleaner to clean too old oplog, which is synced by [OplogSyncer].
#[derive(Debug)]
pub struct OplogCleaner {
    storage_uri: String,
}

impl OplogCleaner {
    pub fn new(storage_uri: String) -> Self {
        OplogCleaner { storage_uri }
    }

    /// clean too old oplog record.
    ///
    /// In detail, this method will clean oplogs before 3 days.
    pub fn run_clean(&self) -> Result<u64> {
        let client = Client::with_uri_str(&self.storage_uri)?;
        let collection: Collection<Document> =
            client.database(LOG_STORAGE_DB).collection(LOG_STORAGE_COLL);
        let latest_timestamp = oplog_helper::get_latest_ts_no_capped(&collection)?.time;
        let latest_datetime = Utc.timestamp(latest_timestamp as i64, 0);

        let earliest_timestamp = latest_datetime
            .checked_sub_signed(Duration::days(3))
            .unwrap()
            .timestamp() as u32;

        info!(?earliest_timestamp, "Begin to clean oplog records...");
        collection
            .delete_many(
                doc! {TIMESTAMP_KEY: {"$lt": Timestamp {time: earliest_timestamp, increment: 0}}},
                None,
            )
            .map(|x| x.deleted_count)
            .map_err(|e| SyncError::from(e))
    }
}
