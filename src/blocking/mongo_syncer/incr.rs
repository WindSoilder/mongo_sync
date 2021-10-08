//! Provide IncrDumper in incremental sync process.

use std::collections::VecDeque;

use bson::{doc, Document, Timestamp};
use mongodb::sync::Client as MongoClient;
use tracing::info;

use crate::{Result, SyncError, COMMAND_OP, OP_KEY, TIMESTAMP_KEY};

use super::oplog_bulk::execute_normal_oplogs;
use crate::cmd_oplog::CmdOplog;

const BATCH_SIZE: usize = 10000;

fn apply_command_log(cmd_log: Document, mongo_conn: &MongoClient) -> Result<()> {
    let cmd_oplog = CmdOplog::from_oplog_doc(&cmd_log)?;
    if let Some(l) = cmd_oplog {
        info!(?l, "begin to apply command oplog...");
        l.apply(mongo_conn)?;
    }
    Ok(())
}

// after the function is invoked, oplogs will be empty.
fn _execute_apply_ops_cmd(oplogs: &mut Vec<Document>, mongo_conn: &MongoClient) -> Result<()> {
    let mut oplogs_to_apply = Vec::with_capacity(oplogs.len());
    oplogs_to_apply.append(oplogs);

    let _exec_result = mongo_conn
        .database("admin")
        .run_command(doc! {"applyOps": oplogs_to_apply}, None)
        .map_err(SyncError::from)?;
    Ok(())
}

/// A mongodb data incremental dumper to dump data to mongodb.
///
/// Basically, when we receive oplogs record from server, we need to use [push_oplogs](IncrDumper::push_oplogs)
/// to push data to dumper's internal buffer, and call [apply_oplogs](IncrDumper::apply_oplogs) to apply oplogs
/// against mongodb.
#[derive(Debug)]
pub struct IncrDumper {
    oplog_batch: VecDeque<Document>,
    mongo_conn: MongoClient,
}

impl IncrDumper {
    /// create a new dumper for `mongo_conn`.
    pub fn new(mongo_conn: MongoClient) -> Self {
        IncrDumper {
            oplog_batch: VecDeque::with_capacity(BATCH_SIZE),
            mongo_conn,
        }
    }

    /// Push given `oplogs`.
    ///
    /// Note that this push operation will not apply these `oplogs`.  To apply `oplogs`, you need to use
    /// [apply_oplogs](IncrDumper::apply_oplogs) after calling this method.
    pub fn push_oplogs(&mut self, oplogs: Vec<Document>) {
        self.oplog_batch.extend(oplogs);
    }

    /// Apply oplogs in dumper.
    ///
    /// Note that this function **may** apply only some of oplogs.  In detail, if you have some oplogs like this:
    ///
    /// | insert log 1  | insert log 2  |  command log 3 |  insert log 4 | update log 5 |
    ///
    /// first call to `apply_oplogs` will only apply 'insert log 1', 'insert log 2', and return ture along with timestamp of log 2.
    /// second call will only apply 'command log 3', and return true along with timestamp of log 3.
    /// final call will apply 'insert log 4' and 'update log 5', and return false along with timestamp of log 5.
    ///
    /// So if you want to apply all oplogs, you have to do something like this:
    /// ```no_run
    /// use mongo_sync::blocking::mongo_syncer::incr::IncrDumper;
    /// use mongodb::sync::Client;
    ///
    /// let mut incr_dumper = IncrDumper::new(Client::with_uri_str("mongodb://localhost:27017").unwrap());
    /// loop {
    ///     let (need_apply_again, latest_applied_ts) = incr_dumper.apply_oplogs().unwrap();
    ///     if !need_apply_again {
    ///         break
    ///     }
    /// }
    /// ```
    pub fn apply_oplogs(&mut self) -> Result<(bool, Timestamp)> {
        let mut latest_ts = Timestamp {
            increment: 0,
            time: 0,
        };

        // iterate oplog_batch until we meet an `command` oplog.
        let mut normal_oplogs = vec![];
        while !self.oplog_batch.is_empty() {
            let one_log = self.oplog_batch.pop_front().unwrap();
            latest_ts = one_log.get_timestamp(TIMESTAMP_KEY)?;
            match one_log.get_str(OP_KEY)? {
                // meet command, flush normal oplogs, and this command log.
                COMMAND_OP => {
                    if !normal_oplogs.is_empty() {
                        info!(?latest_ts, "Begin to apply oplogs... ");
                        execute_normal_oplogs(&mut normal_oplogs, &self.mongo_conn)?;
                        info!(?latest_ts, "Apply oplogs complete... ");
                    }

                    apply_command_log(one_log, &self.mongo_conn)?;
                    return Ok((!self.oplog_batch.is_empty(), latest_ts));
                }
                _ => {
                    // not a command oplog, just push to normal oplogs set.
                    normal_oplogs.push(one_log);
                    // check if next oplog is cmd oplog, it's special condition too.
                    if let Some(next_oplog) = self.oplog_batch.front() {
                        if next_oplog.get_str(OP_KEY)? == COMMAND_OP {
                            // just break is ok, these oplogs will be handled outside the while loop.
                            break;
                        }
                    }
                }
            }
        }

        // all oplogs are normal oplogs (no command oplog in oplog_batch).
        if !normal_oplogs.is_empty() {
            // finally, check normal_oplogs, and apply.
            info!(?latest_ts, "Begin to apply oplogs... ");
            execute_normal_oplogs(&mut normal_oplogs, &self.mongo_conn)?;
            info!(?latest_ts, "Apply oplogs complete... ");
        }
        Ok((!self.oplog_batch.is_empty(), latest_ts))
    }
}
