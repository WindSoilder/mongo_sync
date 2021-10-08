use bson::{doc, Document, Timestamp};
use mongo_sync::OplogCleaner;
use mongodb::sync::{Client, Collection};

struct Context {
    mongo_uri: String,
    mongo_cli: Client,
}

impl Context {
    pub fn new() -> Self {
        let mongo_uri = option_env!("SYNCER_TEST_TARGET").unwrap_or("mongodb://localhost:27018");
        let client = Client::with_uri_str(mongo_uri).unwrap();
        Context {
            mongo_cli: client,
            mongo_uri: mongo_uri.to_string(),
        }
    }

    pub fn get_coll(&self) -> Collection<Document> {
        self.mongo_cli
            .database("source_oplog")
            .collection("source_oplog")
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let db = self.mongo_cli.database("source_oplog");
        db.drop(None).unwrap();
    }
}

#[test]
fn test_oplog_run_clean() {
    let context = Context::new();
    // setup data.
    let coll = context.get_coll();
    coll.insert_many(
        vec![
            doc! {"ts": Timestamp{time: 0, increment: 0}},
            doc! {"ts": Timestamp{time: 3 * 24 * 60 * 60 + 1, increment: 0}},
        ],
        None,
    )
    .unwrap();

    let cleaner = OplogCleaner::new(context.mongo_uri.clone());
    cleaner.run_clean().unwrap();

    assert_eq!(coll.count_documents(None, None).unwrap(), 1);
}

#[test]
fn test_oplog_run_clean_only_one_record() {
    let context = Context::new();
    // setup data.
    let coll = context.get_coll();
    coll.insert_many(vec![doc! {"ts": Timestamp{time: 0, increment: 0}}], None)
        .unwrap();

    let cleaner = OplogCleaner::new(context.mongo_uri.clone());
    cleaner.run_clean().unwrap();

    assert_eq!(coll.count_documents(None, None).unwrap(), 1);
}

#[test]
fn test_oplog_run_clean_all_new() {
    let context = Context::new();
    // setup data.
    let coll = context.get_coll();
    coll.insert_many(
        vec![
            doc! {"ts": Timestamp{time: 1 * 24 * 60 * 60, increment: 0}},
            doc! {"ts": Timestamp{time: 3 * 24 * 60 * 60 + 1, increment: 0}},
        ],
        None,
    )
    .unwrap();

    let cleaner = OplogCleaner::new(context.mongo_uri.clone());
    cleaner.run_clean().unwrap();

    assert_eq!(coll.count_documents(None, None).unwrap(), 2);
}
