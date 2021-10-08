// Only test for IncrDyumper.

use bson::{doc, oid::ObjectId, Document, Timestamp};
use mongodb::sync::Client;
use uuid::Uuid;

use mongo_sync::blocking::mongo_syncer::bson_helper::new_bson_binary;
use mongo_sync::blocking::mongo_syncer::incr::IncrDumper;

struct Context {
    pub client: Client,
    pub dumper: IncrDumper,
}

impl Context {
    pub fn new() -> Self {
        let client = Client::with_uri_str(
            option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        )
        .unwrap();
        let dumper = IncrDumper::new(client.clone());
        Context { client, dumper }
    }

    pub fn get_internal(&mut self) -> (&Client, &mut IncrDumper) {
        (&self.client, &mut self.dumper)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.client.database("syncer_test").drop(None).unwrap();
    }
}

#[test]
fn test_push_oplogs() {
    let mut context = Context::new();
    let (mongo_cli, dumper) = context.get_internal();

    let db = mongo_cli.database("syncer_test");
    db.create_collection("test_coll1", None).unwrap();
    let coll = db.collection::<Document>("test_coll1");

    let one_oplog = doc! {
        "ts" : Timestamp{time: 10, increment: 0},
        "v" : 2,
        "op" : "i",
        "ns" : "syncer_test.test_coll1",
        "ui" : new_bson_binary(Uuid::parse_str("c050283e-3641-4d79-8e82-6665b7a4d19b").unwrap()),
        "o" : {
            "_id" : ObjectId::parse_str("60a74505d6daac52c416bb3f").unwrap(),
            "a": 3
        }
    };
    dumper.push_oplogs(vec![one_oplog.clone()]);
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(!need_again);
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );

    // check the record exists in db.
    let record = coll
        .find_one(
            {
                doc! {"a": 3}
            },
            None,
        )
        .unwrap();
    assert!(record.is_some());

    // apply the oplog twice.
    dumper.push_oplogs(vec![one_oplog.clone()]);
    dumper.apply_oplogs().unwrap();
    assert!(!need_again);
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );
    let counts = coll.count_documents(None, None).unwrap();
    assert_eq!(counts, 1);
}

#[test]
fn test_push_normal_oplog_and_cmd_oplog() {
    let mut context = Context::new();
    let (mongo_cli, dumper) = context.get_internal();
    let db = mongo_cli.database("syncer_test");
    db.create_collection("test_coll1", None).unwrap();
    let one_oplog = doc! {
        "ts" : Timestamp{time: 10, increment: 0},
        "v" : 2,
        "op" : "i",
        "ns" : "syncer_test.test_coll1",
        "ui" : new_bson_binary(Uuid::parse_str("c050283e-3641-4d79-8e82-6665b7a4d19b").unwrap()),
        "o" : {
            "_id" : ObjectId::parse_str("60a74505d6daac52c416bb3f").unwrap(),
            "a": 3
        }
    };
    let one_cmd_oplog = doc! {
        "ts" : Timestamp{time: 10, increment: 2}, "ns": "syncer_test.$cmd", "op": "c", "o": {"create": "coll2"}
    };
    dumper.push_oplogs(vec![one_oplog, one_cmd_oplog]);
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(need_again);
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );

    // check only one oplog is applied.
    let counts = db
        .collection::<Document>("test_coll1")
        .count_documents(None, None)
        .unwrap();
    assert_eq!(counts, 1);

    // check the collection not exists in db.
    let coll_names = mongo_cli
        .database("syncer_test")
        .list_collection_names(None)
        .unwrap();
    let not_exists = coll_names.iter().all(|x| x != "coll2");
    assert!(not_exists);

    // apply again, this time the command oplog is applied.
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(!need_again);
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 10,
            increment: 2
        }
    );
    // check the collection exists in db.
    let coll_names = mongo_cli
        .database("syncer_test")
        .list_collection_names(None)
        .unwrap();
    let exists = coll_names.iter().any(|x| x == "coll2");
    assert!(exists);
}

#[test]
fn test_push_oplogs_when_meet_command() {
    let mut context = Context::new();
    let (mongo_cli, dumper) = context.get_internal();
    let db = mongo_cli.database("syncer_test");
    let one_oplog = doc! {"ts" : Timestamp{time: 10, increment: 0}, "ns": "syncer_test.$cmd", "op": "c", "o": {"create": "coll1"}};
    dumper.push_oplogs(vec![one_oplog]);
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(!need_again);
    assert_eq!(
        latest_ts,
        Timestamp {
            time: 10,
            increment: 0
        }
    );

    // check the collection exists in db.
    let coll_names = mongo_cli
        .database("syncer_test")
        .list_collection_names(None)
        .unwrap();
    let exists = coll_names.iter().any(|x| x == "coll1");
    assert!(exists);
    // clean.
    db.drop(None).unwrap();
}

#[test]
fn test_push_oplogs_already_handle_command() {
    let mut context = Context::new();
    // push a command oplog.
    let (mongo_cli, dumper) = context.get_internal();
    let db = mongo_cli.database("syncer_test");
    db.create_collection("test_coll1", None).unwrap();
    let coll = db.collection::<Document>("test_coll1");
    let one_oplog = doc! {"ts" : Timestamp{time: 10, increment: 0}, "ns": "syncer_test.$cmd", "op": "c", "o": {"create": "coll1"}};
    dumper.push_oplogs(vec![one_oplog]);

    // push a normal oplog.
    let one_oplog = doc! {
        "ts" : Timestamp{time: 10, increment: 0},
        "v" : 2,
        "op" : "i",
        "ns" : "syncer_test.test_coll1",
        "ui" : new_bson_binary(Uuid::parse_str("c050283e-3641-4d79-8e82-6665b7a4d19b").unwrap()),
        "o" : {
            "_id" : ObjectId::parse_str("60a74505d6daac52c416bb3f").unwrap(),
            "a": 3
        }
    };
    dumper.push_oplogs(vec![one_oplog]);
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(need_again);
    assert_eq!(latest_ts, Timestamp{time: 10, increment: 0});
    let counts = coll.count_documents(None, None).unwrap();
    assert_eq!(counts, 0);

    // apply again and check.
    let (need_again, latest_ts) = dumper.apply_oplogs().unwrap();
    assert!(!need_again);
    assert_eq!(latest_ts, Timestamp{time: 10, increment: 0});
    let counts = coll.count_documents(None, None).unwrap();
    assert_eq!(counts, 1);
    db.drop(None).unwrap();
}
