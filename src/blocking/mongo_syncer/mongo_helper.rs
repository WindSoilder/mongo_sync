use super::bson_helper;
use crate::Result;
use bson::doc;
use mongodb::sync::Database;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// get collection uuids from given database.
///
/// When given `colls` is not None, it will return uuid with given colls only.
pub fn get_uuids(db: &Database, colls: &Option<Vec<String>>) -> Result<HashSet<Uuid>> {
    let coll_name_to_uuid = get_coll_name_to_uuid(db)?;
    let uuids: HashSet<Uuid> = coll_name_to_uuid
        .iter()
        .filter_map(|(coll_name, uuid)| match colls {
            None => Some(*uuid),
            Some(colls) => {
                if colls.iter().any(|x| x == coll_name) {
                    Some(*uuid)
                } else {
                    None
                }
            }
        })
        .collect();
    Ok(uuids)
}

/// get mapping between `collection name` and `uuid` in given `database`.
pub fn get_coll_name_to_uuid(db: &Database) -> Result<HashMap<String, Uuid>> {
    let mut name_to_uuid: HashMap<String, Uuid> = HashMap::new();
    let cursor = db.list_collections(doc! {}, None)?;

    // Collection info object:
    // { name: `collection_name`, info: {uuid: `uuid`} }
    for doc in cursor {
        let doc = doc?;
        let coll_name = doc.get_str("name")?;
        let uuid = bson_helper::get_uuid(doc.get_document("info")?, "uuid")?;
        name_to_uuid.insert(coll_name.to_string(), uuid);
    }
    Ok(name_to_uuid)
}

/// generate uuid mapping between source database collection and target database collection
/// we need this mapping to make `applyOps` command works.
pub fn get_uuid_mapping(src_db: &Database, target_db: &Database) -> Result<HashMap<Uuid, Uuid>> {
    let src_name_uuid = get_coll_name_to_uuid(&src_db)?;
    let mut target_name_uuid = get_coll_name_to_uuid(&target_db)?;

    let mut uuid_mapping = HashMap::new();
    for (src_name, src_uuid) in src_name_uuid {
        let target_uuid_maybe = target_name_uuid.remove(&src_name);
        if let Some(target_uuid) = target_uuid_maybe {
            uuid_mapping.insert(src_uuid, target_uuid);
        }
    }
    Ok(uuid_mapping)
}
