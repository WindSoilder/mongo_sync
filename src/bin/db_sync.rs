use clap::Clap;
use mongo_sync::DbSyncConf;
use mongo_sync::MongoSyncer;
use std::path::Path;

use tracing::info;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// source mongodb uri.
    #[clap(short, long)]
    src_uri: String,
    /// target mongodb uri.
    #[clap(short, long)]
    target_uri: String,
    /// mongodb uri which save oplogs, it's saved by `oplog_syncer` binary.
    #[clap(short, long)]
    oplog_storage_uri: String,
    /// database to sync.
    #[clap(short, long)]
    db: String,
    /// collections to sync, default sync all collections inside a database.
    #[clap(short, long)]
    colls: Option<Vec<String>>,
    /// how many threads to sync a database.
    #[clap(long)]
    collection_concurrent: Option<usize>,
    /// how many threads to sync a collection.
    #[clap(long)]
    doc_concurrent: Option<usize>,
    /// log file path, if no specified, all log information will be output to stdout.
    #[clap(long)]
    log_path: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let collector = tracing_subscriber::fmt();
    let (non_blocking, _guard) = match opts.log_path {
        Some(path) => {
            let path = Path::new(&path);
            let dir_name = path.parent().unwrap();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let file_appender = tracing_appender::rolling::daily(dir_name, file_name);
            tracing_appender::non_blocking(file_appender)
        }
        None => tracing_appender::non_blocking(std::io::stdout()),
    };
    collector.with_writer(non_blocking).init();

    let conf: DbSyncConf = DbSyncConf::new(
        opts.src_uri,
        opts.target_uri,
        opts.oplog_storage_uri,
        opts.db,
        opts.colls,
        opts.collection_concurrent,
        opts.doc_concurrent,
    );
    info!("Use the following config to sync database: {:?}", conf);

    let syncer = MongoSyncer::new(&conf);
    info!("Begin to sync database.");
    syncer.sync().unwrap();
    Ok(())
}
