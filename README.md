# mongo_sync
Mongodb realtime synchronizer, which is similar to [py-mongo-sync](https://github.com/caosiyang/py-mongo-sync)

# Features
- Support database level and collection full and concurrent synchronize.  You can use `--collection-concurrent` to define how many threads to sync a database, use `--doc-concurrent` to define how many threads to sync a collection.
- Support oplog based synchronize, so we can synchronize incremental data in realtime.
- Support daily rotation log, you can use it through `--log-path` option.  Or else log information will be output to stdout.

# Support
Mongodb 3.6+ (because of official mongodb driver only support mongodb 3.6+)

# Intall
The recommended way to install mongo_sync is using `cargo`:
```shell
cargo +nightly install mongo_sync
```

You can download [released binary](https://github.com/WindSoilder/mongo_sync/releases/) as well.

# Running tests?
To running integration tests, you need to config `SYNCER_TEST_SOURCE` to a testing mongodb uri, or `mongodb://localhost:27017` will be used.

# Example
To run synchronizer, you need to start oplog_syncer to make a realtime mongodb oplog sync first.

Then you can run `db_sync` to sync database in realtime.

## oplog_syncer
```shell
./target/release/oplog_syncer --src-uri "mongodb://localhost:27017" --oplog-storage-uri "mongodb://localhost:27018/"
```

## db_sync
```shell
db_sync --src-uri "mongodb://localhost:27017/?authSource=admin" --oplog-storage-uri  "mongodb://localhost:27018/?authSource=admin" --target-uri "mongodb://localhost:27019" --db test_db
```

Note that the `--oplog-storage-uri` in oplog_syncer and db_sync must be the same.

# Usage help
## oplog_syncer
```shell
USAGE:
    oplog_syncer [OPTIONS] --src-uri <src-uri> --oplog-storage-uri <oplog-storage-uri>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --log-path <log-path>
            log file path, if not specified, all log information will be output to stdout

    -o, --oplog-storage-uri <oplog-storage-uri>    target oplog storage uri
    -s, --src-uri <src-uri>                        source database uri, must be a mongodb cluster
```

## db_sync
```shell
USAGE:
    db_sync [OPTIONS] --src-uri <src-uri> --target-uri <target-uri> --oplog-storage-uri <oplog-storage-uri> --db <db>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --collection-concurrent <collection-concurrent>    how many threads to sync a database
    -c, --colls <colls>...
            collections to sync, default sync all collections inside a database

    -d, --db <db>                                          database to sync
        --doc-concurrent <doc-concurrent>                  how many threads to sync a collection
        --log-path <log-path>
            log file path, if no specified, all log information will be output to stdout

    -o, --oplog-storage-uri <oplog-storage-uri>
            mongodb uri which save oplogs, it's saved by `oplog_syncer` binary

    -s, --src-uri <src-uri>                                source mongodb uri
    -t, --target-uri <target-uri>                          target mongodb uri
```

# The basic arthitecture diagram
```
      ┌───────────────┐
      │ target db     │
      └───┬───────────┘
          │
     xxxx ▼  xxxxxxx
    xx  ┌───────┐  x
   xx   │db_sync│
  x     └───────┘  x
  xxxxxx  ▲  ▲     x
       xx │  │ xxxx
          │  │
          │  │
Full dump │  │Incr dump
          │  │(Real time)
          │  │
          │  │
          │  │
          │  │         ┌─────────────────────┐
          │  └─────────┤oplog storage db     │
          │            └──────▲──────────────┘
          │          xxxxxxx  │   xxxxx
          │          x ┌──────┴──────┐x
          │          x │Oplog syncer │x   Sync oplog from source cluster
          │          x └──────▲──────┘x   to oplog storage in real time
          │          xxxxxxxxx│ xxxxxxx
          │                   │
          │                   │
          │                   │
          │            ┌──────┴───────┐
          └────────────┤Source cluster│
                       └──────────────┘
```

According to the diagram, you can find that there are 2 basic programs provided by mongo_sync
1. oplog syncer: sync mongodb cluster's oplog to target `oplog storage db`.
2. db sync: sync data from `source cluster` to `target db`.

# Benchmark
It's not strictly benchmark test, I just test it manually.

Scenario:

When source cluster insert 50,000 records, how long the `target_db` can synchronizer these new 50,000 insert.

My testing result:

`db_sync` takes about 50 seconds to sync these update, and `py-mongo-sync` takes about 225 seconds to sync these update.  In general, it's about 3.5x faster than `py-mongo-sync`.

And, please note that `50 seconds` is not accrutely, it highly depends on your database and running machine performance.

# How does the core work?
- oplog_syncer just use [tailable cursor](https://docs.mongodb.com/manual/core/tailable-cursors/) to read mongodb oplogs in realtime.
- db_sync full dump just use multithread with [find](https://docs.mongodb.com/manual/reference/method/db.collection.find/) to read data.
- db_sync incr dump use [oplog](https://docs.mongodb.com/manual/core/replica-set-oplog/) to sync incremental update (CRUD, database command.), and this is why source database must be a cluster.

# Notes
1. In incremental state, for now it only support the following command to be sync (which enough for my personal use.):
- rename collection
- dorp collection
- create collection
- drop indexes
- create indexes
2. I haven't test mongo sharding as target, but it should be ok to work.
3. during running `oplog_syncer`, `oplog storage db` will create and using databse named `source_oplog`, and create and using collection named `source_oplog`.  For now this is hardcoded.
4. during running `db_sync`, target databse will create a new collection named `oplog_records`, it saves the latest oplog timestamp applied to the database.
