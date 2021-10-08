//! Provide structured command type oplog definition.

use bson::{doc, Document};
use mongodb::error::{Error as MongoError, ErrorKind, Result as MongoResult};
use mongodb::sync::Client as MongoClient;
use tracing::warn;

use crate::{Result, SyncError};

/// connection namespace.
#[derive(Debug, PartialEq)]
pub struct CollNs<'a> {
    /// database name.
    db_name: &'a str,
    /// collection name.
    coll_name: &'a str,
}

impl<'a> CollNs<'a> {
    /// construct a namespace from `db_name` and `coll_name`.
    pub fn new(db_name: &'a str, coll_name: &'a str) -> Self {
        CollNs { db_name, coll_name }
    }
}

/// Structured command type oplog definition.
///
/// Basically, use [CmdOplog::from_oplog_doc] to parse mongodb oplog and create this item,
/// and then use [apply](CmdOplog::apply) method to apply this oplog for a mongodb client.
#[derive(Debug, PartialEq)]
pub enum CmdOplog<'a> {
    /// rename collection command.
    RenameCollection {
        /// rename namespace from.
        from: CollNs<'a>,
        /// rename namespace to.
        to: CollNs<'a>,
    },
    /// drop collection command.
    DropCollection(CollNs<'a>),
    /// create collection command.
    CreateCollection(CollNs<'a>),
    /// drop indexes command.
    DropIndexes {
        /// relative namespace to drop index.
        ns: CollNs<'a>,
        /// index name.
        name: &'a str,
    },
    /// create indexes command.
    CreateIndexes {
        /// relative namespace to create index.
        ns: CollNs<'a>,
        /// index key.
        key: &'a Document,
        /// index name.
        name: &'a str,
        /// is unique?.
        unique: bool,
        /// index partial_filter_expression, for more information, see <https://docs.mongodb.com/manual/core/index-partial/>
        partial_filter_expression: Option<&'a Document>,
    },
}

impl<'a> CmdOplog<'a> {
    /// Parse mongodb command oplog to create the item.
    ///
    /// It returns None when the command in `doc` can't be recognized.
    ///
    /// # Example
    /// ```
    /// use mongo_sync::cmd_oplog::{CmdOplog, CollNs};
    /// use bson::doc;
    /// let test_doc = doc! {"ns": "a.$cmd", "o": {"renameCollection": "a.b", "to": "a.c"}};
    /// let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();
    /// assert_eq!(
    ///     oplog,
    ///     CmdOplog::RenameCollection {
    ///         from: CollNs::new("a", "b"),
    ///         to: CollNs::new("a", "c")
    ///     }
    /// );
    /// ```
    pub fn from_oplog_doc(doc: &'a Document) -> Result<Option<Self>> {
        let obj = doc.get_document("o")?;

        let (db, _) = doc
            .get_str("ns")?
            .split_once(".")
            .expect("The namespace key must be split by `.`");

        if obj.contains_key("renameCollection") {
            // rename collection command.
            // obj structure:
            // {"renameCollection": "ns", "to": "ns"}
            let rename_ns = obj.get_str("renameCollection")?;
            let split_res = rename_ns.split_once(".");
            let (from_db, from_coll) = match split_res {
                None => {
                    panic!("Get a invalid `renameCollection` value, which should be split by '.', get {:?}", rename_ns);
                }
                Some(x) => x,
            };

            let to_ns = obj.get_str("to")?;
            let split_res = to_ns.split_once(".");
            let (to_db, to_coll) = match split_res {
                None => {
                    panic!(
                        "Get an invalid `to` value, which should be split by '.', get {:?}",
                        to_ns
                    );
                }
                Some(x) => x,
            };
            Ok(Some(CmdOplog::RenameCollection {
                from: CollNs::new(from_db, from_coll),
                to: CollNs::new(to_db, to_coll),
            }))
        } else if obj.contains_key("drop") {
            // drop collection command.
            // obj structure:
            // { "drop": "coll" }
            let coll = obj.get_str("drop")?;
            Ok(Some(CmdOplog::DropCollection(CollNs::new(db, coll))))
        } else if obj.contains_key("create") {
            // create collection command.
            // obj structure:
            // { "create": "coll" }
            let coll = obj.get_str("create")?;
            Ok(Some(CmdOplog::CreateCollection(CollNs::new(db, coll))))
        } else if obj.contains_key("createIndexes") {
            // create Indexes command.
            // obj structure:
            // { "createIndexes": "coll", "key": {"x": 1}, "name": "index_name", "unique": true, "partialFilterExpression": {"a": {"$ne": null}}}
            // "unique" key is optional.
            // "partialFilterExpression" key is optional.
            let key_result = obj.get_document("key");
            let key = match key_result {
                Err(err) => {
                    warn!(?obj, ?err, "Failed to access `key` field in createIndex command oplog, so the command will be ignored.");
                    return Ok(None);
                }
                Ok(doc) => doc,
            };

            let name_result = obj.get_str("name");
            let name = match name_result {
                Err(err) => {
                    warn!(?obj, ?err, "Failed to access `name` field in CreateIndex command oplog, so the command will be ignored.");
                    return Ok(None);
                }
                Ok(name) => name,
            };

            let partial_filter_expression_name = "partialFilterExpression";
            let partial_filter_expression = if obj.contains_key(partial_filter_expression_name) {
                Some(obj.get_document(partial_filter_expression_name)?)
            } else {
                None
            };

            let coll = obj.get_str("createIndexes").unwrap();
            let unique = obj.get_bool("unique").unwrap_or(false);
            Ok(Some(CmdOplog::CreateIndexes {
                ns: CollNs::new(db, coll),
                key,
                name,
                unique,
                partial_filter_expression,
            }))
        } else if obj.contains_key("dropIndexes") {
            // drop Indexes command.
            // obj structure:
            // { "dropIndexes": "coll", "index": "index_name"}
            let name_result = obj.get_str("index");
            let name = match name_result {
                Err(err) => {
                    warn!(?obj, ?err, "Failed to access `index` field in dropIndexes command oplog, so the command will be ignored.");
                    return Ok(None);
                }
                Ok(n) => n,
            };

            let coll = obj.get_str("dropIndexes").unwrap();
            Ok(Some(CmdOplog::DropIndexes {
                ns: CollNs::new(db, coll),
                name,
            }))
        } else {
            warn!(?doc, "Get a command which can't be handled.");
            Ok(None)
        }
    }

    /// Apply oplog represent in `self` against `mongo_conn`.
    ///
    /// # Example
    /// ```no_run
    /// use mongodb::sync::Client;
    /// use mongo_sync::cmd_oplog::{CmdOplog, CollNs};
    /// let cli = Client::with_uri_str("mongodb://localhost:27017").unwrap();
    /// let cmd_oplog = CmdOplog::DropCollection(CollNs::new("syncer_test", "b"));
    /// cmd_oplog.apply(&cli).unwrap();
    /// ```
    pub fn apply(self, mongo_conn: &MongoClient) -> Result<()> {
        use CmdOplog::*;
        match self {
            DropCollection(ns) => {
                let coll = mongo_conn
                    .database(ns.db_name)
                    .collection::<Document>(ns.coll_name);
                coll.drop(None).map_err(SyncError::from)
            }
            CreateCollection(ns) => {
                let db = mongo_conn.database(ns.db_name);
                let result = db.create_collection(ns.coll_name, None).map(|_| ());

                if cmd_result_is_ok(&result, "already exist") {
                    Ok(())
                } else {
                    result.map_err(SyncError::from)
                }
            }
            RenameCollection { from, to } => {
                // no... no rename collection api. so have to go through `db.run_command` api.
                // rename collection can only runs in admin database.
                let admin_db = mongo_conn.database("admin");
                let result = admin_db
                    .run_command(
                        doc! {
                            "renameCollection": format!("{}.{}", from.db_name, from.coll_name),
                            "to": format!("{}.{}", to.db_name, to.coll_name),
                        },
                        None,
                    )
                    .map(|_| ());

                if cmd_result_is_ok(&result, "not exist") {
                    Ok(())
                } else {
                    result.map_err(SyncError::from)
                }
            }
            DropIndexes { ns, name } => {
                let db = mongo_conn.database(ns.db_name);
                let result = db
                    .run_command(
                        doc! {
                            "dropIndexes": ns.coll_name,
                            "index": name
                        },
                        None,
                    )
                    .map(|_| ());

                if cmd_result_is_ok(&result, "index not found") {
                    Ok(())
                } else {
                    result.map_err(SyncError::from)
                }
            }

            CreateIndexes {
                ns,
                key,
                name,
                unique,
                partial_filter_expression,
            } => {
                let mut index_info = doc! {
                    "key": key, "unique": unique, "name": name
                };
                if let Some(partial_filter_expression) = partial_filter_expression {
                    index_info.insert("partialFilterExpression", partial_filter_expression.clone());
                }
                let db = mongo_conn.database(ns.db_name);
                let indx_doc = doc! {
                    "createIndexes": ns.coll_name,
                    "indexes": [index_info]
                };

                db.run_command(indx_doc, None)?;
                Ok(())
            }
        }
    }
}

fn cmd_result_is_ok<T>(result: &MongoResult<T>, valid_err_msg: &str) -> bool {
    match result {
        Ok(_) => true,
        Err(e) => cmd_err_msg_contains(e, valid_err_msg),
    }
}

fn cmd_err_msg_contains(error: &MongoError, msg: &str) -> bool {
    match error.kind.as_ref() {
        ErrorKind::Command(err) => err.message.to_lowercase().contains(msg),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmd_oplog_rename_collection() {
        let test_doc = doc! {"ns": "a.$cmd", "o": {"renameCollection": "a.b", "to": "a.c"}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(
            oplog,
            CmdOplog::RenameCollection {
                from: CollNs::new("a", "b"),
                to: CollNs::new("a", "c")
            }
        );
    }

    #[test]
    fn test_cmd_oplog_drop_collection() {
        let test_doc = doc! {"ns": "a.$cmd", "o": {"drop": "cc"}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(oplog, CmdOplog::DropCollection(CollNs::new("a", "cc")));
    }

    #[test]
    fn test_cmd_oplog_create_collection() {
        let test_doc = doc! { "ns": "a.$cmd", "o": {"create": "cc"}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(oplog, CmdOplog::CreateCollection(CollNs::new("a", "cc")));
    }

    #[test]
    fn test_cmd_oplog_drop_indexes() {
        let test_doc = doc! {"ns": "a.$cmd", "o": {"dropIndexes": "abc", "index": "aa_1"}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(
            oplog,
            CmdOplog::DropIndexes {
                ns: CollNs::new("a", "abc"),
                name: "aa_1"
            }
        );
    }

    #[test]
    fn test_cmd_oplog_create_indexes() {
        let test_doc = doc! {"ns": "a.$cmd", "o": {"createIndexes": "coll_aa", "key": {"x": 1}, "name": "x_1"}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(
            oplog,
            CmdOplog::CreateIndexes {
                ns: CollNs::new("a", "coll_aa"),
                key: &doc! {"x": 1},
                name: "x_1",
                unique: false,
                partial_filter_expression: None
            }
        );
    }

    #[test]
    fn test_cmd_oplog_create_indexes_contains_unique_and_partial_key() {
        let test_doc = doc! {"ns": "a.$cmd", "o": {"createIndexes": "coll_aa", "key": {"x": 1}, "name": "x_1", "unique": true, "partialFilterExpression": {"a": {"$gt": 1}}}};
        let oplog = CmdOplog::from_oplog_doc(&test_doc).unwrap().unwrap();

        assert_eq!(
            oplog,
            CmdOplog::CreateIndexes {
                ns: CollNs::new("a", "coll_aa"),
                key: &doc! {"x": 1},
                name: "x_1",
                unique: true,
                partial_filter_expression: Some(&doc! {"a": {"$gt": 1}})
            }
        );
    }
}
