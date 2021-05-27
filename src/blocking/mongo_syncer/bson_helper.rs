use bson::document::{ValueAccessError, ValueAccessResult};
use bson::spec::BinarySubtype;
use bson::Binary;
use bson::Bson;
use bson::Document;
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
