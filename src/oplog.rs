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

use mongodb::bson::{Document, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Oplog {
    /// The time when the oplog entry was created.
    ts: Timestamp,
    /// The operation type.
    op: OpType,
    /// The namespace on which to apply the operation.
    ns: String,
    /// The operation applied.
    o: Document,
    /// Additional information about the operation applied.
    o2: Option<Document>,
    /// Version.
    v: i64,
}

/// The type of an operation in the oplog
/// The information comes from: https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/oplog_entry.idl
#[derive(Serialize, Deserialize)]
enum OpType {
    /// represent by "c"
    //Command,
    c,
    /// represent by "i"
    //Insert,
    i,
    /// represent by "u"
    //Update,
    u,
    /// represent by "d"
    //Delete,
    d,
    /// represent by "n", which is useless.
    //Noop,
    n,
    p,
}

impl Oplog {
    pub fn from_doc(doc: Document) -> Option<Oplog> {
        let log: Oplog = bson::from_document(doc).unwrap();
        return None;
    }
}

pub enum Operation {
    Insert {
        ts: Timestamp,
        ns: String,
        o: Document,
        v: i8,
    },
}
