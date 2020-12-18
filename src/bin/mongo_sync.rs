use clap::Clap;
use mongo_sync::{Connection, MongoSyncer, SyncerConfig};
use std::sync::Arc;

#[macro_use]
extern crate log;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// configuration file path.
    #[clap(short, long, default_value = "config.toml")]
    conf: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opts: Opts = Opts::parse();
    let data = std::fs::read(opts.conf).unwrap();
    let conf: SyncerConfig = toml::from_slice(&data).unwrap();
    let syncer = MongoSyncer::new(Connection::new(Arc::new(conf)).unwrap());
    syncer.sync_full();
    Ok(())
}
