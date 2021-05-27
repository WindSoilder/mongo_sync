## Mongodb sync logic.
### For more information, refer to mongodump code.
### Full sync
1. Before sync data, take note for the latest oplog timestamp `A`(from source).
2. Sync documents.
3. Check if `A` still exists in oplog.
4. Write check point `A` to target db.

### Incr sync
1. Get latest oplog timestamp `B`(from source)
2. Fetch oplogs between `A` and `B`
3. Apply oplogs
4. Go back to 1.

### Some corner case consider
#### What if I want to sync more collections...
1. Take note for collection sync arguments.
2. Detect if we need to sync more collections
3. If we need to, we first apply oplogs between checkpoint `A` and latest oplog timestamp `B` (from source)
4. After apply complete, make full sync for these new collections
5. Goes into incremental mode.
