//! Most oplog information comes from here:
//! https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/oplog_entry.idl
//! https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/optime_and_wall_time_base.idl
//! https://github.com/mongodb/mongo/blob/master/src/mongo/db/logical_session_id.idl
//! https://github.com/mongodb/mongo/blob/master/src/mongo/idl/basic_types.idl
//!
//! Useful fields:
//! ts: The time when the oplog entry was created. (Timestamp)
//! op: The operation type. (enum Optype)
//! ns: The namespace on which to apply the operation. (String)
//! o: The operation applied. (Document)
//! o2: Additional information about the operation applied. (Document)
//! v: Version (i64)
//!
//! For `op` field, the definition of OpType:
//! "c": Command
//! "i": Insert
//! "u": Update
//! "d": Delete
//! "n": Noop

use crate::{Result, SyncError};
use mongodb::bson::{oid::ObjectId, Document, Timestamp};

const OP_KEY: &str = "op";
const INSERT_OP: &str = "i";
const UPDATE_OP: &str = "u";
const DELETE_OP: &str = "d";
const COMMAND_OP: &str = "c";
const NOOP_OP: &str = "n";

const TIMESTAMP_KEY: &str = "ts";
const VERSION_KEY: &str = "version";
const NAMESPACE_KEY: &str = "ns";
const OBJ_KEY: &str = "o";
const OBJ2_KEY: &str = "o2";

#[derive(Debug)]
pub struct Oplog {
    ts: Timestamp,
    version: u8,
    op: Operation,
}

#[derive(Debug)]
pub enum Operation {
    Insert {
        namespace: String,
        obj: Document,
    },
    Command {
        namespace: String,
        obj: Document,
        obj2: Option<Document>,
    },
    Update {
        namespace: String,
        obj: Document,
        id_to_update: ObjectId,
    },
    Delete {
        namespace: String,
        id_to_delete: ObjectId,
    },
    Noop,
}

impl Oplog {
    pub fn from_doc(doc: Document) -> Result<Self> {
        // filter invalid namespace
        if Self::is_useless_namespace(doc.get_str(NAMESPACE_KEY)?) {
            // handle this as noop.  It's just useless.
            return Self::make_noop_op(doc);
        }

        match doc.get_str(OP_KEY)? {
            INSERT_OP => Self::make_insert_op(doc),
            UPDATE_OP => Self::make_update_op(doc),
            DELETE_OP => Self::make_delete_op(doc),
            COMMAND_OP => Self::make_command_op(doc),
            NOOP_OP => Self::make_noop_op(doc),
            other => Err(SyncError::BsonValueError {
                key: OP_KEY.to_string(),
                val: other.to_string(),
            }),
        }
    }

    fn make_insert_op(doc: Document) -> Result<Self> {
        Ok(Oplog {
            ts: doc.get_timestamp(TIMESTAMP_KEY)?,
            version: doc.get_i32(VERSION_KEY)? as u8,
            op: Operation::Insert {
                namespace: doc.get_str(NAMESPACE_KEY)?.to_string(),
                obj: doc.get_document(OBJ_KEY)?.clone(),
            },
        })
    }

    fn make_update_op(doc: Document) -> Result<Self> {
        Ok(Oplog {
            ts: doc.get_timestamp(TIMESTAMP_KEY)?,
            version: doc.get_i32(VERSION_KEY)? as u8,
            op: Operation::Update {
                namespace: doc.get_str(NAMESPACE_KEY)?.to_string(),
                id_to_update: doc.get_document(OBJ2_KEY)?.get_object_id("_id")?.clone(),
                obj: doc.get_document(OBJ_KEY)?.clone(),
            },
        })
    }

    fn make_delete_op(doc: Document) -> Result<Self> {
        Ok(Oplog {
            ts: doc.get_timestamp(TIMESTAMP_KEY)?,
            version: doc.get_i32(VERSION_KEY)? as u8,
            op: Operation::Delete {
                namespace: doc.get_str(NAMESPACE_KEY)?.to_string(),
                id_to_delete: doc.get_document(OBJ_KEY)?.get_object_id("_id")?.clone(),
            },
        })
    }

    fn make_command_op(doc: Document) -> Result<Self> {
        let obj2 = match doc.get_document(OBJ2_KEY) {
            Ok(d) => Some(d.clone()),
            Err(_) => None,
        };

        Ok(Oplog {
            ts: doc.get_timestamp(TIMESTAMP_KEY)?,
            version: doc.get_i32(VERSION_KEY)? as u8,
            op: Operation::Command {
                namespace: doc.get_str(NAMESPACE_KEY)?.to_string(),
                obj: doc.get_document(OBJ_KEY)?.clone(),
                obj2,
            },
        })
    }

    fn make_noop_op(doc: Document) -> Result<Self> {
        Ok(Oplog {
            ts: doc.get_timestamp(TIMESTAMP_KEY)?,
            version: doc.get_i32(VERSION_KEY)? as u8,
            op: Operation::Noop,
        })
    }

    fn is_useless_namespace(ns: &str) -> bool {
        // namespace starts with "admin.", "local.", "config." is useless to use.
        ns.starts_with("admin.") || ns.starts_with("local.") || ns.starts_with("config.")
    }
}
