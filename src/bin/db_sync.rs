use clap::Clap;
use mongo_sync::DbSyncConf;
use mongo_sync::{Connection, MongoSyncer};
use std::sync::Arc;
use tracing::info;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    #[clap(short, long)]
    src_uri: String,
    #[clap(short, long)]
    target_uri: String,
    #[clap(short, long)]
    oplog_storage_uri: String,
    #[clap(short, long)]
    db: String,
    #[clap(short, long)]
    colls: Option<Vec<String>>,
    #[clap(long)]
    collection_concurrent: Option<usize>,
    #[clap(long)]
    doc_concurrent: Option<usize>,
    #[clap(long)]
    record_collection: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let collector = tracing_subscriber::fmt().finish();
    tracing::subscriber::set_global_default(collector).expect("setting tracig default failed");
    let opts: Opts = Opts::parse();

    let conf: DbSyncConf = DbSyncConf::new(
        opts.src_uri,
        opts.target_uri,
        opts.oplog_storage_uri,
        opts.db,
        opts.colls,
        opts.collection_concurrent,
        opts.doc_concurrent,
        opts.record_collection,
    );
    info!("Use the following config to sync database: {:?}", conf);

    let syncer = MongoSyncer::new(Connection::new(Arc::new(conf)).unwrap());
    info!("Begin to sync database.");
    syncer.sync().unwrap();
    Ok(())
}
