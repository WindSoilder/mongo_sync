use bson::{doc, oid::ObjectId, Document};
use mongo_sync::blocking::mongo_syncer::oplog_bulk::execute_normal_oplogs;
use mongodb::sync::Client;

struct Context {
    pub client: Client,
}

impl Context {
    pub fn new() -> Self {
        let client = Client::with_uri_str(
            option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        )
        .unwrap();
        Context { client }
    }

    pub fn get_internal(&self) -> &Client {
        &self.client
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.client.database("syncer_test").drop(None).unwrap();
    }
}

#[test]
fn test_execute_normal_oplogs() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");

    let test_id = ObjectId::new();
    // apply insert oplog.
    let mut insert_oplogs =
        vec![doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}}];
    execute_normal_oplogs(&mut insert_oplogs, client).unwrap();
    // check.
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 2});

    // apply update oplog.
    let mut update_oplogs = vec![
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o2": {"_id": test_id}, "o": {"$v": 1, "$set": {"a": 3}}},
    ];
    execute_normal_oplogs(&mut update_oplogs, client).unwrap();
    // check.
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 3});

    // apply delete oplog.
    let mut delete_oplogs =
        vec![doc! {"op": "d", "ns": "syncer_test.test_coll", "o": {"_id": test_id}}];
    execute_normal_oplogs(&mut delete_oplogs, client).unwrap();
    // check
    let result = test_coll.find_one(doc! {"_id": test_id}, None).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_execute_normal_oplogs_with_many_operation_type() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");

    let test_id = ObjectId::new();
    let test_id2 = ObjectId::new();
    let mut oplogs = vec![
        doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}},
        doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id2, "a": 3}},
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 10}, "o2": {"_id": test_id}},
        doc! {"op": "d", "ns": "syncer_test.test_coll", "o": {"_id": test_id2}},
    ];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    // check
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 10});
    let result = test_coll.find_one(doc! {"_id": test_id2}, None).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_execute_normal_oplogs_with_different_namespace() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");
    let test_coll2 = client
        .database("syncer_test")
        .collection::<Document>("test_coll2");

    let test_id = ObjectId::new();
    let test_id2 = ObjectId::new();
    let mut oplogs = vec![
        doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}},
        doc! {"op": "i", "ns": "syncer_test.test_coll2", "o": {"_id": test_id2, "a": 3}},
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 10}, "o2": {"_id": test_id}},
    ];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    // check
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 10});
    let result = test_coll2
        .find_one(doc! {"_id": test_id2}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id2, "a": 3});
}

#[test]
fn test_execute_normal_oplogs_with_many_operation_type_and_different_namespace() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");
    let test_coll2 = client
        .database("syncer_test")
        .collection::<Document>("test_coll2");

    let test_id = ObjectId::new();
    let test_id2 = ObjectId::new();
    let mut oplogs = vec![
        doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}},
        doc! {"op": "i", "ns": "syncer_test.test_coll2", "o": {"_id": test_id2, "a": 3}},
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 10}, "o2": {"_id": test_id}},
        doc! {"op": "d", "ns": "syncer_test.test_coll2", "o": {"_id": test_id2}},
    ];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    // check
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 10});
    let result = test_coll2.find_one(doc! {"_id": test_id2}, None).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_execute_normal_oplogs_idempotently() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");
    let test_id = ObjectId::new();

    let mut oplogs =
        vec![doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}}];
    execute_normal_oplogs(&mut oplogs.clone(), client).unwrap();
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 2});

    let mut oplogs = vec![
        doc! {"op": "i", "ns": "syncer_test.test_coll", "o": {"_id": test_id, "a": 2}},
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o2": {"_id": test_id}, "o": {"$v": 1, "$set": {"d": 3}}},
    ];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    let result = test_coll
        .find_one(doc! {"_id": test_id}, None)
        .unwrap()
        .unwrap();
    assert_eq!(result, doc! {"_id": test_id, "a": 2, "d": 3});
}

#[test]
fn test_execute_normal_oplogs_update_op_with_no_exists_record() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");
    let test_id = ObjectId::new();

    let mut oplogs = vec![
        doc! {"op": "u", "ns": "syncer_test.test_coll", "o2": {"_id": test_id}, "o": {"$v": 1, "$set": {"d": 3}}},
    ];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    let result = test_coll.find_one(doc! {"_id": test_id}, None).unwrap();

    assert!(result.is_none());
}

#[test]
fn test_execute_normal_oplogs_delete_op_with_no_exists_record() {
    let context = Context::new();
    let client = context.get_internal();
    client
        .database("syncer_test")
        .create_collection("test_coll", None)
        .unwrap();
    let test_coll = client
        .database("syncer_test")
        .collection::<Document>("test_coll");
    let test_id = ObjectId::new();

    let mut oplogs = vec![doc! {"op": "d", "ns": "syncer_test.test_coll", "o": {"_id": test_id}}];
    execute_normal_oplogs(&mut oplogs, client).unwrap();
    let result = test_coll.find_one(doc! {"_id": test_id}, None).unwrap();

    assert!(result.is_none());
}
