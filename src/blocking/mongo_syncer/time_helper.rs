use chrono::{DateTime, Local, TimeZone};
use mongodb::bson::Timestamp;

pub fn to_datetime(ts: &Timestamp) -> DateTime<Local> {
    Local.timestamp(ts.time as i64, ts.increment)
}
