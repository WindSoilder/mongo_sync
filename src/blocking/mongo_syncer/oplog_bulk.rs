//! Provide something similar to oplog `bulkWrite` feature.

use crate::{Result, SyncError, NAMESPACE_KEY, OP_KEY};
use bson::{doc, Document};
use mongodb::sync::Client as MongoClient;
use std::collections::HashMap;
use tracing::{info, warn};

/// Execute normal CRUD `oplogs` against `mongo_conn` connection.
///
/// Must make sure that given `oplogs` doesn't contains `c` operation.  Or it will be ignored.
pub fn execute_normal_oplogs(oplogs: &mut Vec<Document>, mongo_conn: &MongoClient) -> Result<()> {
    // convert from oplog to relative operation is inspired from py-mongo-sync:
    // https://github.com/caosiyang/py-mongo-sync/blob/master/mongosync/multi_oplog_replayer.py
    //
    // But mongo rust driver doesn't contains `bulkWrite` api, so we have to borrow `buldWrite` api
    // implementation, in detail, we need to construct the `update` and `delete` operation command
    // manually.
    let mut oplogs_to_write = vec![];
    oplogs_to_write.append(oplogs);
    let mut statement_docs = vec![];

    let mut current_op = oplogs_to_write[0].get_str(OP_KEY).unwrap().to_string();
    let db_name = oplogs_to_write[0]
        .get_str(NAMESPACE_KEY)
        .unwrap()
        .split_once(".")
        .unwrap()
        .0
        .to_string();
    for mut one_log in oplogs_to_write.into_iter() {
        let obj = one_log.remove("o");
        let mut obj = match obj {
            Some(bson::Bson::Document(d)) => d,
            _ => panic!("Mongodb oplog `o` attribute should be a document"),
        };

        let op = one_log.get_str(OP_KEY)?;
        // here we inject collection name to every statement document, then we
        // can easily tell mongodb apply statement to which colleciton.
        let coll_name = one_log.get_str(NAMESPACE_KEY)?.split_once(".").unwrap().1;

        // need to flush logs.
        if _need_to_flush(op, &current_op) {
            _flush_oplogs(&current_op, &mut statement_docs, &db_name, mongo_conn)?;
            current_op = op.to_string();
        }

        // for more information about update and delete command:
        // https://docs.mongodb.com/manual/reference/command/update/
        // https://docs.mongodb.com/manual/reference/command/delete/
        //
        // mongodb behavior:
        // if update, generate a 'u' oplog.
        // if update with successfully upsert, generate a 'i' oplog.
        // if insert, generate a 'i' oplog.
        // if delete, generate a 'd' oplog.
        match op {
            // update operation.
            "u" => {
                let is_update = obj.keys().any(|x| x.starts_with("$"));
                if is_update {
                    // $v is only for mongodb internal usage, don't send this key to server.
                    obj.remove("$v");
                };
                // make compiler happy, assign `coll_name` again.
                let coll_name = one_log.get_str(NAMESPACE_KEY)?.split_once(".").unwrap().1;
                statement_docs.push(doc! {
                    "q": {"_id": one_log.get_document("o2")?.get_object_id("_id")?},
                    "u": obj,
                    "upsert": !is_update,
                    "coll_name": coll_name
                })
            }
            // insert operation, because we need the oplog replay idempotently.
            // we will convert it to an `update` command.
            "i" => statement_docs.push(doc! {
                "q": {"_id": obj.get_object_id("_id")?},
                "u": obj,
                "upsert": true,
                "coll_name": coll_name
            }),
            // delete operation.
            "d" => statement_docs.push(doc! {
                "q": {"_id": obj.get_object_id("_id")?},
                "limit": 1,
                "coll_name": coll_name
            }),
            _ => {
                warn!(?one_log, "unknown oplog operation, ignored.");
            }
        }
    }

    if !statement_docs.is_empty() {
        _flush_oplogs(&current_op, &mut statement_docs, &db_name, mongo_conn)?;
    }
    Ok(())
}

fn _need_to_flush(op: &str, current_op: &str) -> bool {
    let update_ops = ["i", "u"];
    update_ops.contains(&current_op) ^ update_ops.contains(&op)
}

// after flush, `oplogs` will be empty.
fn _flush_oplogs(
    op: &str,
    statement_docs: &mut Vec<Document>,
    db_name: &str,
    mongo_conn: &MongoClient,
) -> Result<()> {
    let mut coll_ops = HashMap::new();
    let (command, update_doc_key) = if op == "d" {
        ("delete", "deletes")
    } else {
        ("update", "updates")
    };

    let mut oplogs_to_apply = vec![];
    oplogs_to_apply.append(statement_docs);
    for mut one_log in oplogs_to_apply {
        let coll_name = one_log.remove("coll_name").unwrap();
        let coll_name = match coll_name {
            bson::Bson::String(c) => c,
            _ => panic!("collection name must be a string"),
        };

        if !coll_ops.contains_key(&coll_name) {
            coll_ops.insert(coll_name.to_string(), vec![]);
        }
        coll_ops.get_mut(&coll_name).unwrap().push(one_log);
    }

    let db = mongo_conn.database(db_name);
    for (coll_name, oplogs) in coll_ops.into_iter() {
        info!(%coll_name, operation=%command, "Flush oplogs for collection.");
        // `ordered` key default to be true, so we don't need to tell mongodb explicitly.
        let result = db.run_command(
            doc! {
                command: coll_name,
                update_doc_key: oplogs
            },
            None,
        )?;

        if result.contains_key("writeErrors") {
            return Err(SyncError::ApplyOplogError(result));
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_need_to_flush() {
        // two same operation.
        assert!(!_need_to_flush(&"u".to_string(), &"u".to_string()));
        assert!(!_need_to_flush(&"i".to_string(), &"i".to_string()));
        assert!(!_need_to_flush(&"d".to_string(), &"d".to_string()));

        // one 'u' operation, one 'i' operation.
        assert!(!_need_to_flush(&"u".to_string(), &"i".to_string()));
        assert!(!_need_to_flush(&"i".to_string(), &"u".to_string()));

        // one 'u' or 'i' operation, one 'd' operation.
        assert!(_need_to_flush(&"u".to_string(), &"d".to_string()));
        assert!(_need_to_flush(&"d".to_string(), &"u".to_string()));
        assert!(_need_to_flush(&"i".to_string(), &"d".to_string()));
        assert!(_need_to_flush(&"d".to_string(), &"i".to_string()));
    }
}
