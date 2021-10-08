use bson::{doc, Document};
use mongo_sync::blocking::mongo_syncer::full;
use mongodb::options::FindOneOptions;
use mongodb::sync::{Client, Database};
use rayon::ThreadPoolBuilder;
use std::sync::Arc;

struct Context {
    pub(crate) source_db: Database,
    pub(crate) target_db: Database,
}

impl Context {
    fn new(src_uri: &str, target_uri: &str) -> Self {
        let source_db = Client::with_uri_str(src_uri)
            .unwrap()
            .database("syncer_test_source");
        let target_db = Client::with_uri_str(target_uri)
            .unwrap()
            .database("syncer_test_target");
        Self {
            source_db,
            target_db,
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.source_db.drop(None).unwrap();
        self.target_db.drop(None).unwrap();
    }
}

#[test]
fn test_sync_one_concurrent() {
    let context = Context::new(
        option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        option_env!("SYNCER_TEST_TARGET").unwrap_or("mongodb://localhost:27018"),
    );
    let pool = Arc::new(ThreadPoolBuilder::new().num_threads(2).build().unwrap());
    let source_coll = context
        .source_db
        .collection::<Document>("syncer_test_source");
    let target_coll = context
        .source_db
        .collection::<Document>("syncer_test_target");
    // setup.
    let docs: Vec<Document> = (0..20000).map(|_| doc! {"a": 3}).collect();
    source_coll.insert_many(docs, None).unwrap();

    // execute.
    full::sync_one_concurrent(source_coll, target_coll.clone(), 2, pool).unwrap();
    // check result in target collection.
    assert_eq!(target_coll.count_documents(None, None).unwrap(), 20000);
    for d in target_coll.find(None, None).unwrap() {
        let mut item = d.unwrap();
        item.remove("_id");
        assert_eq!(item, doc! {"a": 3});
    }
}

#[test]
fn test_sync_one_serial() {
    let context = Context::new(
        option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        option_env!("SYNCER_TEST_TARGET").unwrap_or("mongodb://localhost:27018"),
    );
    // setup.
    let docs: Vec<Document> = (0..20000).map(|_| doc! {"a": 3}).collect();
    let source_coll = context
        .source_db
        .collection::<Document>("syncer_test_source");
    let target_coll = context
        .source_db
        .collection::<Document>("syncer_test_target");
    source_coll.insert_many(docs, None).unwrap();
    // execute.
    full::sync_one_serial(source_coll, target_coll.clone()).unwrap();
    // check result in target collection.
    assert_eq!(target_coll.count_documents(None, None).unwrap(), 20000);
    for d in target_coll.find(None, None).unwrap() {
        let mut item = d.unwrap();
        item.remove("_id");
        assert_eq!(item, doc! {"a": 3});
    }
}

#[test]
fn test_split_ids() {
    let context = Context::new(
        option_env!("SYNCER_TEST_SOURCE").unwrap_or("mongodb://localhost:27017"),
        option_env!("SYNCER_TEST_TARGET").unwrap_or("mongodb://localhost:27018"),
    );
    // setup.
    let docs: Vec<Document> = (0..20000).map(|_| doc! {"a": 3}).collect();
    let source_coll = context
        .source_db
        .collection::<Document>("syncer_test_source");
    source_coll.insert_many(docs, None).unwrap();

    // split_ids.
    let result = full::split_ids(&source_coll, 1).unwrap();
    assert_eq!(result.len(), 1);
    let min_id = source_coll
        .find_one(
            None,
            FindOneOptions::builder().sort(doc! {"_id": 1}).build(),
        )
        .unwrap()
        .unwrap()
        .get_object_id("_id")
        .unwrap();
    let max_id = source_coll
        .find_one(
            None,
            FindOneOptions::builder().sort(doc! {"_id": -1}).build(),
        )
        .unwrap()
        .unwrap()
        .get_object_id("_id")
        .unwrap();
    assert_eq!(result[0].0, min_id);
    assert_eq!(result[0].1, max_id);

    // split by 10 concurrent.
    let result = full::split_ids(&source_coll, 10).unwrap();
    let global_cnt: u64 = result
        .into_iter()
        .map(|(min_id, max_id)| {
            source_coll
                .count_documents(doc! {"_id": {"$gte": min_id, "$lte": max_id}}, None)
                .unwrap()
        })
        .sum();
    assert_eq!(global_cnt, 20000);
}
