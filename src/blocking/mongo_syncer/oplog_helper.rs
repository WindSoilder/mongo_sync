use crate::{Result, SyncError, TIMESTAMP_KEY};
use bson::{doc, Timestamp};
use mongodb::options::FindOneOptions;
use mongodb::sync::Collection;

enum Natural {
    Earliest,
    Latest,
}

pub fn get_earliest_ts(coll: &Collection) -> Result<Timestamp> {
    get_one_oplog_ts(coll, Natural::Earliest)
}

pub fn get_latest_ts(coll: &Collection) -> Result<Timestamp> {
    get_one_oplog_ts(coll, Natural::Latest)
}

fn get_one_oplog_ts(coll: &Collection, natural: Natural) -> Result<Timestamp> {
    let sorted_doc = match natural {
        Natural::Earliest => doc! {"$natural": 1},
        Natural::Latest => doc! {"$natural": -1},
    };

    coll.find_one(None, FindOneOptions::builder().sort(sorted_doc).build())?
        .map(|d| {
            d.get_timestamp(TIMESTAMP_KEY)
                .map_err(|e| SyncError::BsonError(e))
        })
        .unwrap_or_else(|| Err(SyncError::EmptyDocError))
}
