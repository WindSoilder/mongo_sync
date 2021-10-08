use crate::{Result, SyncError, COMMAND_OP, NAMESPACE_KEY, OP_KEY, TIMESTAMP_KEY};
use bson::Document;
use bson::{doc, Timestamp};
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::sync::Collection;
use std::collections::HashSet;

enum Natural {
    Earliest,
    Latest,
}

/// get earliest timestamp from mongodb `oplog.rs`, which is identified by `coll`.
pub fn get_earliest_ts(coll: &Collection<Document>) -> Result<Timestamp> {
    get_one_oplog_ts(coll, Natural::Earliest)
}

/// get latest timestamp from mongodb `oplog.rs`, which is identified by `coll`.
pub fn get_latest_ts(coll: &Collection<Document>) -> Result<Timestamp> {
    get_one_oplog_ts(coll, Natural::Latest)
}

fn get_one_oplog_ts(coll: &Collection<Document>, natural: Natural) -> Result<Timestamp> {
    let sorted_doc = match natural {
        Natural::Earliest => doc! {"$natural": 1},
        Natural::Latest => doc! {"$natural": -1},
    };

    coll.find_one(None, FindOneOptions::builder().sort(sorted_doc).build())?
        .map(|d| d.get_timestamp(TIMESTAMP_KEY).map_err(SyncError::from))
        .unwrap_or_else(|| Err(SyncError::EmptyDocError))
}

/// get earliest timestamp for no capped collection `coll`, the collection must have the same schema as `oplog.rs`.
pub fn get_earliest_ts_no_capped(coll: &Collection<Document>) -> Result<Timestamp> {
    get_one_oplog_ts_no_capped(coll, Natural::Earliest)
}

/// get latest timestamp for no capped collection `coll`, the collection must have the same schema as `oplog.rs`.
pub fn get_latest_ts_no_capped(coll: &Collection<Document>) -> Result<Timestamp> {
    get_one_oplog_ts_no_capped(coll, Natural::Latest)
}

fn get_one_oplog_ts_no_capped(coll: &Collection<Document>, natural: Natural) -> Result<Timestamp> {
    let sorted_doc = match natural {
        Natural::Earliest => doc! {TIMESTAMP_KEY: 1},
        Natural::Latest => doc! {TIMESTAMP_KEY: -1},
    };

    coll.find_one(None, FindOneOptions::builder().sort(sorted_doc).build())?
        .map(|d| d.get_timestamp(TIMESTAMP_KEY).map_err(SyncError::from))
        .unwrap_or_else(|| Err(SyncError::EmptyDocError))
}

/// get next oplog batch in given collection.
///
/// oplog batch will be fetched from `oplog_coll`, starts with `start_point`, if end_point is not None, it will
/// fetch until given `end_point`.  The returned batch will not excess `size` limit.
pub fn get_next_batch(
    oplog_coll: &Collection<Document>,
    start_point: Timestamp,
    end_point: Option<Timestamp>,
    size: usize,
) -> Result<Vec<Document>> {
    let mut filter = doc! {"ts": {"$gt": start_point}};
    if let Some(end_ts) = end_point {
        filter
            .get_document_mut("ts")
            .unwrap()
            .insert("$lte", end_ts);
    }

    let mut result = vec![];
    for doc in oplog_coll.find(
        filter,
        FindOptions::builder()
            .sort(doc! {TIMESTAMP_KEY: 1})
            .limit(size as i64)
            .build(),
    )? {
        let d = doc?;
        result.push(d);
    }
    Ok(result)
}

/// filter `oplogs` which database of namespace is inside `db_name`, and collection of namespace should inside `valid_colls`.
///
/// If `valid_colls` is None, `oplogs` only filter by `db_name`.  Note that any oplogs match `db_name`
/// with command op will always be valid.
///
/// # Example
/// ```rust
/// use bson::{Document, doc};
/// use std::collections::HashSet;
/// use mongo_sync::blocking::mongo_syncer::oplog_helper;
///
/// // need database "a" oplog.
/// let oplogs = vec![doc!{"ns": "a.b", "op": "i"}, doc!{"ns": "b.c", "op": "d"}];
/// let oplogs = oplog_helper::filter_oplogs(oplogs, "a", &None);
/// assert_eq!(oplogs, vec![doc!{"ns": "a.b", "op": "i"}]);
///
/// // need database "a", collection "b" and "c" oplog.
/// let oplogs = vec![doc!{"ns": "a.b", "op": "i"}, doc!{"ns": "a.d", "op": "u"}, doc!{"ns": "a.c", "op": "d"}];
/// let valid_colls: HashSet<String> = vec!["b".to_string(), "c".to_string()].into_iter().collect();
/// let oplogs = oplog_helper::filter_oplogs(oplogs, "a", &Some(valid_colls));
/// assert_eq!(oplogs, vec![doc!{"ns": "a.b", "op": "i"}, doc!{"ns": "a.c", "op": "d"}]);
///
/// // command oplog with `db_name` will always be valid
/// let valid_colls: HashSet<String> = vec!["b".to_string(), "c".to_string()].into_iter().collect();
/// let oplogs = vec![doc!{"ns": "a.$cmd", "op": "c"}, doc!{"ns": "a.d", "op": "u"}];
/// let oplogs = oplog_helper::filter_oplogs(oplogs, "a", &Some(valid_colls));
/// assert_eq!(oplogs, vec![doc!{"ns": "a.$cmd", "op": "c"}]);
/// ```
pub fn filter_oplogs(
    oplogs: Vec<Document>,
    db_name: &str,
    valid_colls: &Option<HashSet<String>>,
) -> Vec<Document> {
    oplogs
        .into_iter()
        .filter(|one_log| {
            let (log_db_name, log_coll_name) = one_log
                .get_str(NAMESPACE_KEY)
                .expect("oplog should contains `ns` key")
                .split_once(".")
                .expect("`ns` value should be split by '.'");
            if log_db_name == db_name {
                match valid_colls {
                    Some(valid_colls) => {
                        (one_log
                            .get_str(OP_KEY)
                            .expect("oplog should contains `op` field")
                            == COMMAND_OP)
                            || valid_colls.contains(log_coll_name)
                    }
                    None => true,
                }
            } else {
                false
            }
        })
        .collect()
}
