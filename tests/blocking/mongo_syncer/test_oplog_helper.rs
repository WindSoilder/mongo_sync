use bson::{doc, Document, Timestamp};
use mongodb::options::CreateCollectionOptions;
use mongodb::sync::{Client, Collection};

use mongo_sync::blocking::mongo_syncer::oplog_helper::{
    get_earliest_ts, get_latest_ts, get_next_batch,
};

struct Context {
    pub client: Client,
    pub capped_coll: Collection<Document>,
    pub no_capped_coll: Collection<Document>,
}

impl Context {
    pub fn new() -> Self {
        let client = Client::with_uri_str(
            option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        )
        .unwrap();
        let db = client.database("syncer_test");
        db.create_collection(
            "syncer_test_capped",
            CreateCollectionOptions::builder()
                .capped(true)
                .size(10)
                .build(),
        )
        .unwrap();
        let capped_coll = client
            .database("syncer_test")
            .collection::<Document>("syncer_test_capped");
        let no_capped_coll = client
            .database("syncer_test")
            .collection::<Document>("syncer_test_no_capped");
        Context {
            client,
            capped_coll,
            no_capped_coll,
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.client.database("syncer_test").drop(None).unwrap();
    }
}

#[test]
fn test_get_earliest_ts() {
    let context = Context::new();
    let coll = &context.capped_coll;
    coll.insert_one(doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}}, None)
        .unwrap();
    coll.insert_one(
        doc! {"b": 3, "ts": Timestamp{time: 13, increment: 10}},
        None,
    )
    .unwrap();

    let earliest_ts = get_earliest_ts(&coll).unwrap();
    assert_eq!(
        earliest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );
}

#[test]
fn test_get_earliest_ts_when_oplog_not_exists() {
    let context = Context::new();
    let coll = &context.capped_coll;
    let result = get_earliest_ts(&coll);
    assert!(result.is_err());
}

#[test]
fn test_get_latest_ts() {
    let context = Context::new();
    let coll = &context.capped_coll;
    coll.insert_one(doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}}, None)
        .unwrap();
    coll.insert_one(
        doc! {"b": 3, "ts": Timestamp{time: 13, increment: 10}},
        None,
    )
    .unwrap();

    let latest_ts = get_latest_ts(&coll).unwrap();
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 13,
            increment: 10
        }
    );
}

#[test]
fn test_get_latest_ts_when_oplog_not_exists() {
    let context = Context::new();
    let coll = &context.capped_coll;

    let result = get_latest_ts(&coll);
    assert!(result.is_err());
}

#[test]
fn test_get_earliest_ts_no_capped() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    coll.insert_one(doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}}, None)
        .unwrap();
    coll.insert_one(
        doc! {"b": 3, "ts": Timestamp{time: 13, increment: 10}},
        None,
    )
    .unwrap();

    let earliest_ts = get_earliest_ts(&coll).unwrap();
    assert_eq!(
        earliest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );
}

#[test]
fn test_get_earliest_ts_no_capped_when_oplog_not_exists() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    let result = get_earliest_ts(&coll);
    assert!(result.is_err());
}

#[test]
fn test_get_latest_ts_no_capped() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    coll.insert_one(doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}}, None)
        .unwrap();
    coll.insert_one(
        doc! {"b": 3, "ts": Timestamp{time: 13, increment: 10}},
        None,
    )
    .unwrap();

    let latest_ts = get_latest_ts(&coll).unwrap();
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 13,
            increment: 10
        }
    );
}

#[test]
fn test_get_latest_ts_no_capped_when_oplog_not_exists() {
    let context = Context::new();
    let coll = &context.no_capped_coll;

    let result = get_latest_ts(&coll);
    assert!(result.is_err());
}

#[test]
fn test_get_next_batch() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    coll.insert_many(
        vec![
            doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}},
            doc! {"b": 2, "ts": Timestamp{time: 11, increment: 0}},
        ],
        None,
    )
    .unwrap();

    let next_batch = get_next_batch(
        coll,
        Timestamp {
            time: 9,
            increment: 0,
        },
        None,
        2,
    )
    .unwrap()
    .into_iter()
    .map(|mut d| {
        d.remove("_id").unwrap();
        d
    })
    .collect::<Vec<Document>>();

    assert_eq!(
        next_batch,
        vec![
            doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}},
            doc! {"b": 2, "ts": Timestamp{time: 11, increment: 0}},
        ]
    );
}

#[test]
fn test_get_next_batch_not_include_start_point() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    coll.insert_many(
        vec![
            doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}},
            doc! {"b": 2, "ts": Timestamp{time: 11, increment: 0}},
        ],
        None,
    )
    .unwrap();

    let next_batch = get_next_batch(
        coll,
        Timestamp {
            time: 10,
            increment: 0,
        },
        Some(Timestamp {
            time: 11,
            increment: 0,
        }),
        2,
    )
    .unwrap()
    .into_iter()
    .map(|mut d| {
        d.remove("_id").unwrap();
        d
    })
    .collect::<Vec<Document>>();

    assert_eq!(
        next_batch,
        vec![doc! {"b": 2, "ts": Timestamp{time: 11, increment: 0}},]
    );
}

#[test]
fn test_get_next_batch_size_limit() {
    let context = Context::new();
    let coll = &context.no_capped_coll;
    coll.insert_many(
        vec![
            doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}},
            doc! {"b": 2, "ts": Timestamp{time: 11, increment: 0}},
        ],
        None,
    )
    .unwrap();
    let next_batch = get_next_batch(
        coll,
        Timestamp {
            time: 9,
            increment: 0,
        },
        Some(Timestamp {
            time: 11,
            increment: 0,
        }),
        1,
    )
    .unwrap()
    .into_iter()
    .map(|mut d| {
        d.remove("_id").unwrap();
        d
    })
    .collect::<Vec<Document>>();

    assert_eq!(
        next_batch,
        vec![doc! {"a": 1, "ts": Timestamp{time: 10, increment: 0}},]
    );
}
