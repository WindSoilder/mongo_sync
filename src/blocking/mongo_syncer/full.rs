use crate::error::{Result, SyncError};
use bson::doc;
use bson::oid::ObjectId;
use bson::Document;
use crossbeam::channel;
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::sync::Collection;
use rayon::ThreadPool;
use std::sync::Arc;
use tracing::info;

/// Internal message which is used by [sync_one_concurrent].
pub enum SyncTableStatus {
    /// sync part of table success.
    Done,
    /// sync part of table failed, along with error message.
    Failed(SyncError),
}

/// Sync one collection from `source_coll` to `target_coll` concurrently.
///
/// During sync progress, new threads will be allocated by `pool`, and there will be max to `doc_concurrent` threads.
pub fn sync_one_concurrent(
    source_coll: Collection<Document>,
    target_coll: Collection<Document>,
    doc_concurrent: usize,
    pool: Arc<ThreadPool>,
) -> Result<()> {
    info!(collection_name=%source_coll.name(), "Full state: Begin to sync collection concurrently. ");
    // get ObjectId range for each threads.
    let id_ranges: Vec<(ObjectId, ObjectId)> = split_ids(&source_coll, doc_concurrent)?;
    let buf_size = 10000;
    let (sender, receiver) = channel::bounded(doc_concurrent);

    for (id_min, id_max) in id_ranges {
        let source_coll = source_coll.clone();
        let target_coll = target_coll.clone();
        let sender = sender.clone();
        pool.spawn(move || {
            let mut buffer = Vec::with_capacity(buf_size);
            let res = source_coll
                .find(
                    Some(doc! {"_id": {"$gte": id_min, "$lte": id_max}}),
                    FindOptions::builder().batch_size(10000).build(),
                )
                .and_then(|cursor| {
                    for doc in cursor {
                        buffer.push(doc.unwrap());
                        if buffer.len() == buf_size {
                            let mut data_to_write = Vec::with_capacity(buf_size);
                            data_to_write.append(&mut buffer);
                            target_coll.insert_many(data_to_write, None)?;
                        }
                    }
                    if !buffer.is_empty() {
                        target_coll.insert_many(buffer, None)?;
                    }

                    Ok(())
                });

            match res {
                Err(e) => {
                    let _ = sender.send(SyncTableStatus::Failed(SyncError::MongoError {
                        source: e,
                        backtrace: std::backtrace::Backtrace::capture(),
                    }));
                }
                Ok(_) => {
                    let _ = sender.send(SyncTableStatus::Done);
                }
            };
        })
    }

    let mut count = 0;
    while let Ok(event) = receiver.recv() {
        match event {
            SyncTableStatus::Failed(e) => return Err(e),
            SyncTableStatus::Done => {
                count += 1;
                if count == doc_concurrent {
                    break;
                }
            }
        }
    }
    info!(collection_name=%source_coll.name(), "Full state: Finish sync collection concurrently. ");
    Ok(())
}

/// split collection into a list of ObjectId range pair.
pub fn split_ids(
    coll: &Collection<Document>,
    doc_concurrent: usize,
) -> Result<Vec<(ObjectId, ObjectId)>> {
    let count = coll.count_documents(None, None)? as usize;

    let mut id_ranges: Vec<(ObjectId, ObjectId)> = Vec::with_capacity(doc_concurrent);
    let docs_per_worker = count / doc_concurrent;
    let null_id_bytes = [0; 12];
    for i in 0..doc_concurrent - 1 {
        // We can assume that the doc is always exists.
        let mut min_id = coll
            .find_one(
                None,
                FindOneOptions::builder()
                    .sort(doc! {"_id": 1})
                    .skip(Some((i * docs_per_worker) as u64))
                    .build(),
            )?
            .unwrap();
        let mut max_id = coll
            .find_one(
                None,
                FindOneOptions::builder()
                    .sort(doc! {"_id": 1})
                    .skip(Some(((i + 1) * docs_per_worker) as u64 - 1))
                    .build(),
            )?
            .unwrap();

        let min_id = min_id.get_object_id_mut("_id").unwrap();
        let min_id = std::mem::replace(min_id, ObjectId::from_bytes(null_id_bytes));
        let max_id = max_id.get_object_id_mut("_id").unwrap();
        let max_id = std::mem::replace(max_id, ObjectId::from_bytes(null_id_bytes));
        id_ranges.push((min_id, max_id));
    }

    // last worker get remain ids.
    let mut last_min_id = coll
        .find_one(
            None,
            FindOneOptions::builder()
                .sort(doc! {"_id": 1})
                .skip(Some(((doc_concurrent - 1) * docs_per_worker) as u64))
                .build(),
        )?
        .unwrap();
    let last_min_id = last_min_id.get_object_id_mut("_id").unwrap();
    let last_min_id = std::mem::replace(last_min_id, ObjectId::from_bytes(null_id_bytes));
    let mut last_max_id = coll
        .find_one(
            None,
            FindOneOptions::builder().sort(doc! {"_id": -1}).build(),
        )?
        .unwrap();
    let last_max_id = last_max_id.get_object_id_mut("_id").unwrap();
    let last_max_id = std::mem::replace(last_max_id, ObjectId::from_bytes(null_id_bytes));
    id_ranges.push((last_min_id, last_max_id));
    Ok(id_ranges)
}

/// Synchronize mongodb collection from `source_coll` to `target_coll` serial.
pub fn sync_one_serial(
    source_coll: Collection<Document>,
    target_coll: Collection<Document>,
) -> Result<()> {
    info!(collection_name=%source_coll.name(), "Full state: Finish sync collection serial. ");
    let buf_size = 10000;
    let mut buffer = Vec::with_capacity(buf_size);
    let cursor = source_coll
        .find(None, FindOptions::builder().batch_size(10000).build())
        .unwrap();
    for doc in cursor {
        buffer.push(doc.unwrap());
        if buffer.len() == buf_size {
            let mut data_to_write = Vec::with_capacity(buf_size);
            data_to_write.append(&mut buffer);
            target_coll.insert_many(data_to_write, None)?;
        }
    }

    if !buffer.is_empty() {
        target_coll.insert_many(buffer, None)?;
    }
    info!(collection_name=%source_coll.name(), "Full state: Finish sync collection serial. ");
    Ok(())
}
