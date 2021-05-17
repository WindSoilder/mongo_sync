# mongo_sync
Mongodb oplog based syncer.

# Intall
```shell
cargo install mongo_sync
```

# Usage example
```shell
mongo_sync -f
```

# Features
to be continue...

# Notes on mongodb repl implementation
## Life as a Secondary

In general, secondaries just choose a node to sync from, their **sync source**, and then pull
operations from its oplog and apply those oplog entries to their own copy of the data on disk.

Secondaries also constantly update their sync source with their progress so that the primary can
satisfy write concerns.

### Oplog Fetching

A secondary keeps its data synchronized with its sync source by fetching oplog entries from its sync
source. This is done via the
[`OplogFetcher`](https://github.com/mongodb/mongo/blob/929cd5af6623bb72f05d3364942e84d053ddea0d/src/mongo/db/repl/oplog_fetcher.h).

The `OplogFetcher` does not directly apply the operations it retrieves from the sync source.
Rather, it puts them into a buffer (the **`OplogBuffer`**) and another thread is in charge of
taking the operations off the buffer and applying them. That buffer uses an in-memory blocking
queue for steady state replication; there is a similar collection-backed buffer used for initial
sync.

#### Oplog Fetcher Lifecycle

The `OplogFetcher` is owned by the
[`BackgroundSync`](https://github.com/mongodb/mongo/blob/r4.2.0/src/mongo/db/repl/bgsync.h) thread.
The `BackgroundSync` thread runs continuously while a node is in `SECONDARY` state.
`BackgroundSync` sits in a loop, where each iteration it first chooses a sync source with the
`SyncSourceResolver` and then starts up the `OplogFetcher`.

In steady state, the `OplogFetcher` continuously receives and processes batches of oplog entries
from its sync source.

The `OplogFetcher` could terminate because the first batch implies that a rollback is required, it
could receive an error from the sync source, or it could just be shut down by its owner, such as
when `BackgroundSync` itself is shut down. In addition, after every batch, the `OplogFetcher` runs
validation checks on the documents in that batch. It then decides if it should continue syncing
from the current sync source. If validation fails, or if the node decides to stop syncing, the
`OplogFetcher` will shut down.

When the `OplogFetcher` terminates, `BackgroundSync` restarts sync source selection, exits, or goes
into ROLLBACK depending on the return status.

#### Oplog Fetcher Implementation Details

Let’s refer to the sync source as node A and the fetching node as node B.

After starting up, the `OplogFetcher` first creates a connection to sync source A. Through this
connection, it will establish an **exhaust cursor** to fetch oplog entries. This means that after
the initial `find` and `getMore` are sent, A will keep sending all subsequent batches without
needing B to run any additional `getMore`s.

The `find` command that B’s `OplogFetcher` first sends to sync source A has a greater than or equal
predicate on the timestamp of the last oplog entry it has fetched. The original `find` command
should always return at least 1 document due to the greater than or equal predicate. If it does
not, that means that A’s oplog is behind B's and thus A should not be B’s sync source. If it does
return a non-empty batch, but the first document returned does not match the last entry in B’s
oplog, there are two possibilities. If the oldest entry in A's oplog is newer than B's latest
entry, that means that B is too stale to sync from A. As a result, B denylists A as a sync source
candidate. Otherwise, B's oplog has diverged from A's and it should go into
[**ROLLBACK**](https://docs.mongodb.com/manual/core/replica-set-rollbacks/).

After getting the original `find` response, secondaries check the metadata that accompanies the
response to see if the sync source is still a good sync source. Secondaries check that the node has
not rolled back since it was chosen and that it is still ahead of them.

The `OplogFetcher` specifies `awaitData: true, tailable: true` on the cursor so that subsequent
batches block until their `maxTimeMS` expires waiting for more data instead of returning
immediately. If there is no data to return at the end of `maxTimeMS`, the `OplogFetcher` receives
an empty batch and will wait on the next batch.

If the `OplogFetcher` encounters any errors while trying to connect to the sync source or get a
batch, it will use `OplogFetcherRestartDecision` to check that it has enough retries left to create
a new cursor. The connection class will automatically handle reconnecting to the sync source when
needed. Whenever the `OplogFetcher` successfully receives a batch, it will reset its retries. If it
errors enough times in a row to exhaust its retries, that might be an indication that there is
something wrong with the connection or the sync source. In that case, the `OplogFetcher` will shut
down with an error status.

The `OplogFetcher` may shut down for a variety of other reasons as well. After each successful
batch, the `OplogFetcher` decides if it should continue syncing from the current sync source. If
the `OplogFetcher` decides to continue, it will wait for the next batch to arrive and repeat. If
not, the `OplogFetcher` will terminate, which will lead to `BackgroundSync` choosing a new sync
source. Reasons for changing sync sources include:

* If the node is no longer in the replica set configuration.
* If the current sync source is no longer in the replica set configuration.
* If the user has requested another sync source via the `replSetSyncFrom` command.
* If chaining is disabled and the node is not currently syncing from the primary.
* If the sync source is not the primary, does not have its own sync source, and is not ahead of
  the node. This indicates that the sync source will not receive writes in a timely manner. As a
  result, continuing to sync from it will likely cause the node to be lagged.
* If the most recent OpTime of the sync source is more than `maxSyncSourceLagSecs` seconds behind
  another member's latest oplog entry. This ensures that the sync source is not too far behind
  other nodes in the set. `maxSyncSourceLagSecs` is a server parameter and has a default value of
  30 seconds.
* If the node has discovered another eligible sync source that is significantly closer. A
  significantly closer node has a ping time that is at least `changeSyncSourceThresholdMillis`
  lower than our current sync source. This minimizes the number of nodes that have sync sources
  located far away.`changeSyncSourceThresholdMillis` is a server parameter and has a default value
  of 5 ms.

### Oplog Entry Application

A separate thread, `ReplBatcher`, runs the
[`OplogBatcher`](https://github.com/mongodb/mongo/blob/r4.3.6/src/mongo/db/repl/oplog_batcher.h) and
is used for pulling oplog entries off of the oplog buffer and creating the next batch that will be
applied. These batches are called **oplog applier batches** and are different from **oplog fetcher
batches**, which are sent by a node's sync source during [oplog fetching](#oplog-fetching). Oplog
applier batches differ from oplog fetcher batches because they have more restrictions than just size
limits when creating a new batch. Operations in a batch are applied in parallel when possible, so
there are certain operation types (like commands) which require being in their own oplog applier
batch. For example, a dropDatabase operation shouldn't be applied in parallel with other operations,
so it must be in a batch of size one.

The
[`OplogApplier`](https://github.com/mongodb/mongo/blob/r4.2.0/src/mongo/db/repl/oplog_applier.h)
is in charge of applying each batch of oplog entries received from the batcher. It will run in an
endless loop doing the following:

1. Get the next oplog applier batch from the batcher.
2. Acquire the [Parallel Batch Writer Mode lock](#parallel-batch-writer-mode).
3. Set the [`oplogTruncateAfterPoint`](#replication-timestamp-glossary) to the node's last applied
   optime (before this batch) to aid in [startup recovery](#startup-recovery) if the node shuts down
   in the middle of writing entries to the oplog.
4. Write the batch of oplog entries into the oplog.
5. Clear the `oplogTruncateAfterPoint` and set the [**`minValid`**](#replication-timestamp-glossary)
   document to be the optime of the last entry in the batch. Until the node applies entries through
   the optime set in this document, the data will not be consistent with the oplog.
6. Use multiple threads to apply the batch in parallel. This means that oplog entries within the
   same batch are not necessarily applied in order. The operations in each batch will be divided
   among the writer threads. The only restriction for creating the vector of operations that each
   writer thread will apply serially has to do with the namespace that the operation applies to.
   Operations on a document must be atomic and ordered, so operations on the same namespace will be
   put on the same thread to be serialized. When applying operations, each writer thread will try to
   **group** together insert operations for improved performance and will apply all other operations
   individually.
7. Tell the storage engine to flush the journal.
8. Persist the node's "applied through" optime (the optime of the last oplog entry in this oplog
   applier batch) to disk. This will update the `minValid` document now that the batch has been
   applied in its entirety.
9. Update [**oplog visibility**](../catalog/README.md#oplog-visibility) by notifying the storage
   engine of the new oplog entries. Since entries in an oplog applier batch are applied in
   parallel, it is only safe to make these entries visible once all the entries in this batch are
   applied, otherwise an oplog hole could be made visible.
10. Finalize the batch by advancing the global timestamp (and the node's last applied optime) to the
   last optime in the batch.

## Initial Sync

Initial sync is the process that we use to add a new node to a replica set. Initial sync is
initiated by the `ReplicationCoordinator` and done in the
[**`InitialSyncer`**](https://github.com/mongodb/mongo/blob/r4.2.0/src/mongo/db/repl/initial_syncer.h).
When a node begins initial sync, it goes into the `STARTUP2` state. `STARTUP` is reserved for the
time before the node has loaded its local configuration of the replica set.

At a high level, there are two phases to initial sync: the [**data clone phase**](#data-clone-phase)
and the [**oplog application phase**](#oplog-application-phase). During the data clone phase, the
node will copy all of another node's data. After that phase is completed, it will start the oplog
application phase where it will apply all the oplog entries that were written since it started
copying data. Finally, it will reconstruct any transactions in the prepared state.

Before the data clone phase begins, the node will do the following:

1. Set the initial sync flag to record that initial sync is in progress and make it durable. If a
   node restarts while this flag is set, it will restart initial sync even though it may already
   have data because it means that initial sync didn't complete. We also check this flag to prevent
   reading from the oplog while initial sync is in progress.
2. Find a sync source.
3. Drop all of its data except for the local database and recreate the oplog.
4. Get the Rollback ID (RBID) from the sync source to ensure at the end that no rollbacks occurred
   during initial sync.
5. Query its sync source's oplog for its latest OpTime and save it as the
   `defaultBeginFetchingOpTime`. If there are no open transactions on the sync source, this will be
   used as the `beginFetchingTimestamp` or the timestamp that it begins fetching oplog entries from.
6. Query its sync source's transactions table for the oldest starting OpTime of all active
   transactions. If this timestamp exists (meaning there is an open transaction on the sync source)
   this will be used as the `beginFetchingTimestamp`. If this timestamp doesn't exist, the node will
   use the `defaultBeginFetchingOpTime` instead. This will ensure that even if a transaction was
   started on the sync source after it was queried for the oldest active transaction timestamp, the
   syncing node will have all the oplog entries associated with an active transaction in its oplog.
7. Query its sync source's oplog for its lastest OpTime. This will be the `beginApplyingTimestamp`,
   or the timestamp that it begins applying oplog entries at once it has completed the data clone
   phase. If there was no active transaction on the sync source, the `beginFetchingTimestamp` will
   be the same as the `beginApplyingTimestamp`.
8. Create an `OplogFetcher` and start fetching and buffering oplog entries from the sync source
   to be applied later. Operations are buffered to a collection so that they are not limited by the
   amount of memory available.

### Data clone phase

The new node then begins to clone data from its sync source. The `InitialSyncer` constructs an
[`AllDatabaseCloner`](https://github.com/mongodb/mongo/blob/r4.3.2/src/mongo/db/repl/all_database_cloner.h)
that's used to clone all of the databases on the upstream node. The `AllDatabaseCloner` asks the
sync source for a list of its databases and then for each one it creates and runs a
[`DatabaseCloner`](https://github.com/mongodb/mongo/blob/r4.3.2/src/mongo/db/repl/database_cloner.h)
to clone that database. Each `DatabaseCloner` asks the sync source for a list of its collections and
for each one creates and runs a
[`CollectionCloner`](https://github.com/mongodb/mongo/blob/r4.3.2/src/mongo/db/repl/collection_cloner.h)
to clone that collection. The `CollectionCloner` calls `listIndexes` on the sync source and creates
a
[`CollectionBulkLoader`](https://github.com/mongodb/mongo/blob/r4.3.2/src/mongo/db/repl/collection_bulk_loader.h)
to create all of the indexes in parallel with the data cloning. The `CollectionCloner` then uses an
**exhaust cursor** to run a `find` request on the sync source for each collection, inserting the
fetched documents each time, until it fetches all of the documents. Instead of explicitly needing to
run a `getMore` on an open cursor to get the next batch, exhaust cursors make it so that if the
`find` does not exhaust the cursor, the sync source will keep sending batches until there are none
left.

The cloners are resilient to transient errors.  If a cloner encounters an error marked with the
`RetriableError` label in
[`error_codes.yml`](https://github.com/mongodb/mongo/blob/r4.3.2/src/mongo/base/error_codes.yml), it
will retry whatever network operation it was attempting.  It will continue attempting to retry for a
length of time set by the server parameter `initialSyncTransientErrorRetryPeriodSeconds`, after
which it will consider the failure permanent.  A permanent failure means it will choose a new sync
source and retry all of initial sync, up to a number of times set by the server parameter
`numInitialSyncAttempts`.  One notable exception, where we do not retry the entire operation, is for
the actual querying of the collection data.  For querying, we use a feature called **resume
tokens**.  We set a flag on the query: `$_requestResumeToken`.  This causes each batch we receive
from the sync source to contain an opaque token which indicates our current position in the
collection.  After storing a batch of data, we store the most recent resume token in a member
variable of the `CollectionCloner`.  Then, when retrying we provide this resume token in the query,
allowing us to avoid having to re-fetch the parts of the collection we have already stored.

The `initialSyncTransientErrorRetryPeriodSeconds` is also used to control retries for the oplog
fetcher and all network operations in initial sync which take place after the data cloning has
started.

### Oplog application phase

After the cloning phase of initial sync has finished, the oplog application phase begins. The new
node first asks its sync source for its last applied OpTime and this is saved as the
`stopTimestamp`, the oplog entry it must apply before it's consistent and can become a secondary. If
the `beginFetchingTimestamp` is the same as the `stopTimestamp`, then it indicates that there are no
oplog entries that need to be written to the oplog and no operations that need to be applied. In
this case, the node will seed its oplog with the last oplog entry applied on its sync source and
finish initial sync.

Otherwise, the new node iterates through all of the buffered operations, writes them to the oplog,
and if their timestamp is after the `beginApplyingTimestamp`, applies them to the data on disk.
Oplog entries continue to be fetched and added to the buffer while this is occurring.

One notable exception is that the node will not apply `prepareTransaction` oplog entries. Similar
to how we reconstruct prepared transactions in startup and rollback recovery, we will update the
transactions table every time we see a `prepareTransaction` oplog entry. Because the nodes wrote
all oplog entries starting at the `beginFetchingTimestamp` into the oplog, the node will have all
the oplog entries it needs to
[reconstruct the state for all prepared transactions](#recovering-prepared-transactions) after the
oplog application phase is done.

### Idempotency concerns

Some of the operations that are applied may already be reflected in the data that was cloned since
we started buffering oplog entries before the collection cloning phase even started. Consider the
following:

1. Start buffering oplog entries
2. Insert `{a: 1, b: 1}` to collection `foo`
3. Insert `{a: 1, b: 2}` to collection `foo`
4. Drop collection `foo`
5. Recreate collection `foo`
6. Create unique index on field `a` in collection `foo`
7. Clone collection `foo`
8. Start applying oplog entries and try to insert both `{a: 1, b: 1}` and `{a: 1, b: 2}`

As seen here, there can be operations on collections that have since been dropped or indexes could
conflict with the data being added. As a result, many errors that occur here are ignored and assumed
to resolve themselves, such as `DuplicateKey` errors (like in the example above).

### Finishing initial sync

The oplog application phase concludes when the node applies an oplog entry at `stopTimestamp`. The
node checks its sync source's Rollback ID to see if a rollback occurred and if so, restarts initial
sync. Otherwise, the `InitialSyncer` will begin tear down.

It will register the node's [`lastApplied`](#replication-timestamp-glossary) OpTime with the storage
engine to make sure that all oplog entries prior to that will be visible when querying the oplog.
After that it will reconstruct all prepared transactions. The node will then clear the initial sync
flag and tell the storage engine that the [`initialDataTimestamp`](#replication-timestamp-glossary)
is the node's last applied OpTime. Finally, the `InitialSyncer` shuts down and the
`ReplicationCoordinator` starts steady state replication.
