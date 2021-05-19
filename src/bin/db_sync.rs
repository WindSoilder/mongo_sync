use clap::Clap;
use mongo_sync::{Connection, MongoSyncer, SyncerConfig};
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
    db: String,
    #[clap(short, long)]
    colls: Option<Vec<String>>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let collector = tracing_subscriber::fmt().finish();
    tracing::subscriber::set_global_default(collector).expect("setting tracig default failed");
    let opts: Opts = Opts::parse();
    println!("{:?}", opts);
    /*
    let data = std::fs::read(opts.conf).unwrap();
    let conf: SyncerConfig = toml::from_slice(&data).unwrap();
    let syncer = MongoSyncer::new(Connection::new(Arc::new(conf)).unwrap());
    syncer.sync()?;
    Ok(())*/
    Ok(())
}
