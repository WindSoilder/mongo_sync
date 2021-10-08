use bson::document::{ValueAccessError, ValueAccessResult};
use bson::spec::BinarySubtype;
use bson::Binary;
use bson::Bson;
use bson::Document;
use uuid::Uuid;

/// Get a uuid value for for this `key` if it exists and has the corrent type for given `doc`
///
/// # Panic
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
pub fn new_bson_binary(uuid: Uuid) -> Binary {
    Binary {
        subtype: BinarySubtype::Uuid,
        bytes: uuid.as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bson::doc;

    #[test]
    fn test_get_uuid() {
        let test_id = Uuid::new_v4();
        assert_eq!(
            get_uuid(&doc! {"a": new_bson_binary(test_id)}, "a").unwrap(),
            test_id
        );
    }

    #[test]
    fn test_get_uuid_when_key_not_exists() {
        let test_id = Uuid::new_v4();

        assert!(get_uuid(&doc! {"a": new_bson_binary(test_id)}, "b").is_err());
    }

    #[test]
    fn test_get_uuid_when_key_is_not_valid_type() {
        assert!(get_uuid(&doc! {"a": "bbbb"}, "a").is_err());
    }
}
