## Mongodb sync logic.
### For more information, refer to mongodump code.
### Full sync
1. Before sync data, take note for the latest oplog timestamp `A`(from source).
2. Sync documents.
3. After sync documents complete, take a note for the latest oplog timestamp `B`(from source)
4. fetch all oplog between `timestamp A` and `timestamp B`
5. Check if oplog between `A` and `B` valid, this can be accomplish by fetch the earliest oplog time, and check if it's less than `A`.
6. Apply timestamp between `A` and `B`.

TODO: What if I want to sync more collections...
1. For fetching node, we have a latest oplog timestamp `B`
2. Full sync documents in collections.
3. Apply oplog after `B`

### Incr sync
Fetch oplog from source and apply..
