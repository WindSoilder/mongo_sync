use crate::{Result, SyncError, TIMESTAMP_KEY};
use bson::{doc, Timestamp};
use mongodb::options::FindOneOptions;
use mongodb::sync::Collection;
use tracing::info;

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
        .map(|d| d.get_timestamp(TIMESTAMP_KEY).map_err(SyncError::BsonError))
        .unwrap_or_else(|| Err(SyncError::EmptyDocError))
}

pub fn get_earliest_ts_no_capped(coll: &Collection) -> Result<Timestamp> {
    get_one_oplog_ts_no_capped(coll, Natural::Earliest)
}

pub fn get_latest_ts_no_capped(coll: &Collection) -> Result<Timestamp> {
    get_one_oplog_ts_no_capped(coll, Natural::Latest)
}

fn get_one_oplog_ts_no_capped(coll: &Collection, natural: Natural) -> Result<Timestamp> {
    let sorted_doc = match natural {
        Natural::Earliest => doc! {TIMESTAMP_KEY: 1},
        Natural::Latest => doc! {TIMESTAMP_KEY: -1},
    };

    coll.find_one(None, FindOneOptions::builder().sort(sorted_doc).build())?
        .map(|d| d.get_timestamp(TIMESTAMP_KEY).map_err(SyncError::BsonError))
        .unwrap_or_else(|| Err(SyncError::EmptyDocError))
}

/// get a point after the `start_point` to fetch oplogs in our copied oplog collection.
///
/// When this function returns the `end_point`, it will make sure that the length of `oplog` between
/// `start_point` and `end_point` will not excess `size`.
pub fn get_end_point(
    oplog_coll: &Collection,
    start_point: Timestamp,
    size: usize,
) -> Result<Timestamp> {
    let find_one_options = FindOneOptions::builder()
        .sort(doc! {TIMESTAMP_KEY: -1})
        .build();
    let mut db_end_point = oplog_coll
        .find_one(doc! {}, find_one_options.clone())?
        .unwrap()
        .get_timestamp(TIMESTAMP_KEY)
        .expect("oplog should contains a key named `ts`");

    // In the whole collection, the latest oplog just at start_point.
    // There are just 1 record between start_point and db_end_point.
    if db_end_point == start_point {
        return Ok(db_end_point);
    }

    let valid_end_point = loop {
        let cnt = oplog_coll.count_documents(
            doc! {"ts": {"$gte": start_point, "$lte": db_end_point}},
            None,
        )?;
        if cnt > size as i64 {
            info!(?start_point, ?db_end_point, %cnt, "Too many oplogs, going to make end_time smaller.");

            if db_end_point.time == start_point.time {
                // This happened when we have too many oplogs in at the same time.
                // So `db_end_point.increment - start_point.increment > size`.
                // we can just return back a timestamp with increment value `start_point.increment + size - 1`.
                // and oplog with this timestamp must exists in the databse.
                break Timestamp {
                    time: start_point.time,
                    increment: start_point.increment + size as u32 - 1,
                };
            }

            // should adjust db_end_point to get less oplogs to apply.
            let mut end_point_guess = Timestamp {
                time: (db_end_point.time + start_point.time) / 2,
                increment: u32::MAX,
            };

            db_end_point = loop {
                let next_point = oplog_coll
                    .find_one(
                        doc! {"ts": {"$lte": end_point_guess}},
                        find_one_options.clone(),
                    )?
                    .unwrap()
                    .get_timestamp(TIMESTAMP_KEY)?;
                if next_point != start_point {
                    // there are more data between `start_point` and `next_point`.
                    break next_point;
                } else {
                    // no data with `start_point < ts < end_point_guess`.
                    // Should make `end_point_guess` larger.
                    if db_end_point.time - end_point_guess.time == 1 {
                        // This happended when `db_end_point.time - start_piont.time == 1`
                        // so `end_point_guess.time == start_point.time`.  It indicates we have too many oplogs
                        // in this `db_end_point.time` second (and `db_end_point.increment > size`)
                        // we can just set `next_end_point.increment` as `size`.
                        break Timestamp {
                            time: db_end_point.time,
                            increment: size as u32 - 1,
                        };
                    } else {
                        // improve our guess, make `end_point_guess.time` larger.
                        end_point_guess = Timestamp {
                            time: (end_point_guess.time + db_end_point.time) / 2,
                            increment: u32::MAX,
                        }
                    }
                }
            };
        } else {
            break db_end_point;
        }
    };

    Ok(valid_end_point)
}
