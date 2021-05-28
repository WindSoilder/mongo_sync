use crate::Result;
use bson::document::{ValueAccessError, ValueAccessResult};
use bson::spec::BinarySubtype;
use bson::Binary;
use bson::Bson;
use bson::Document;
use std::collections::HashMap;
use uuid::Uuid;

/// Get a uuid value for for this `key` if it exists and has the corrent type for given `doc`
///
/// ## Panic
/// This function will panic if the `key` in `doc` is an invalid uuid.
pub fn get_uuid(doc: &Document, key: &str) -> ValueAccessResult<Uuid> {
    match doc.get(key) {
        Some(&Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            ref bytes,
        })) => Ok(Uuid::from_slice(bytes).expect("Invalid uuid bytes")),
        Some(_) => Err(ValueAccessError::UnexpectedType),
        None => Err(ValueAccessError::NotPresent),
    }
}

/// Create a new bson::Binary from given `uuid`.
pub fn new_binary(uuid: Uuid) -> Binary {
    Binary {
        subtype: BinarySubtype::Uuid,
        bytes: uuid.as_bytes().to_vec(),
    }
}

/// change oplog `ui` field according to `uuid_mapping`.
pub fn map_oplog_uuids(
    oplogs: &mut Vec<Document>,
    uuid_mapping: &HashMap<Uuid, Uuid>,
) -> Result<()> {
    for op in oplogs.iter_mut() {
        let uuid = get_uuid(op, "ui")?;
        if uuid_mapping.contains_key(&uuid) {
            op.insert("ui", new_binary(*uuid_mapping.get(&uuid).unwrap()));
        } else {
            // TODO: This should not happened....
        }
    }
    Ok(())
}
