use crate::Result;
use bson::doc;
use mongodb::bson::{Document, Timestamp};
use mongodb::options::{CursorType, FindOptions};
use mongodb::sync::{Client, Collection};
use std::time::SystemTime;
use tracing::info;

#[derive(Debug)]
pub struct OplogSyncer {
    source_conn: Client,
    storage_conn: Client,
}

const LOG_STORAGE_DB: &str = "source_oplog";
const LOG_STORAGE_COLL: &str = "source_oplog";
const REPLICA_OPLOG_DB: &str = "local";
const REPLICA_OPLOG_COLL: &str = "oplog.rs";
const OPLOG_TRUNCATE_AFTER_POINT: &str = "oplog_truncate_after_point";

const NAMESPACE_KEY: &str = "ns";
const TIMESTAMP_KEY: &str = "ts";
const OP_KEY: &str = "op";
const NOOP_OP: &str = "n";

impl OplogSyncer {
    pub fn new(src_uri: &str, storage_uri: &str) -> Result<OplogSyncer> {
        let source_conn = Client::with_uri_str(src_uri)?;
        let storage_conn = Client::with_uri_str(storage_uri)?;

        Ok(OplogSyncer {
            source_conn,
            storage_conn,
        })
    }

    pub fn sync_forever(self) -> Result<()> {
        let storage_db = self.storage_conn.database(LOG_STORAGE_DB);
        // [TODO]: create index for storage_coll.
        let storage_coll = storage_db.collection(LOG_STORAGE_COLL);
        let oplog_truncate_after_point = storage_db.collection(OPLOG_TRUNCATE_AFTER_POINT);

        let source_coll = self
            .source_conn
            .database(REPLICA_OPLOG_DB)
            .collection(REPLICA_OPLOG_COLL);

        const BATCH_DELAY: u64 = 3; // save data every 3 seconds.

        // remove dirty data, some oplogs may exists after our recorded truncate point.
        const MIN_TS: Timestamp = Timestamp {
            time: 0,
            increment: 0,
        };
        let truncate_ts = oplog_truncate_after_point
            .find_one(None, None)?
            .unwrap_or(doc! {"ts": MIN_TS})
            .get_timestamp(TIMESTAMP_KEY)?;
        info!(?truncate_ts, "Truncate oplog after given point. ");
        storage_coll.delete_many(doc! {TIMESTAMP_KEY: {"$gte": truncate_ts}}, None)?;
        info!(start_time = ?truncate_ts, "Begin to sync oplog. ");

        // fetch and sync oplog.
        let cursor = if truncate_ts == MIN_TS {
            source_coll.find(
                doc! {TIMESTAMP_KEY: {"$gte": truncate_ts}},
                FindOptions::builder()
                    .cursor_type(CursorType::TailableAwait)
                    .build(),
            )?
        } else {
            source_coll.find(
                doc! {TIMESTAMP_KEY: {"$gte": truncate_ts}},
                FindOptions::builder()
                    .cursor_type(CursorType::TailableAwait)
                    .build(),
            )?
        };

        let mut now = SystemTime::now();
        let mut oplog_batched: Vec<Document> = vec![];
        for doc in cursor {
            let doc = doc?;

            if !self.is_useless_oplog(&doc)? {
                oplog_batched.push(doc);
            }

            if now.elapsed().unwrap().as_secs() >= BATCH_DELAY && !oplog_batched.is_empty() {
                let latest_ts =
                    oplog_batched[oplog_batched.len() - 1].get_timestamp(TIMESTAMP_KEY)?;
                let earliest_ts = oplog_batched[0].get_timestamp(TIMESTAMP_KEY)?;

                let mut data_to_write: Vec<Document> = Vec::with_capacity(oplog_batched.len());
                std::mem::swap(&mut oplog_batched, &mut data_to_write);
                storage_coll.insert_many(data_to_write, None)?;

                info!(?earliest_ts, ?latest_ts, "Sync oplog complete. ");
                oplog_batched.clear();
                self.save_latest_ts(&oplog_truncate_after_point, latest_ts)?;

                info!(?latest_ts, "Write truncate after point complete. ");

                now = SystemTime::now();
            }
        }
        Ok(())
    }

    fn save_latest_ts(
        &self,
        oplog_truncate_after_point: &Collection,
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
}
