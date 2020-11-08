use clap::Clap;
use mongo_sync::{DbConnection, SyncerConfig};
use mongodb::Client;
use std::fs;

use mongodb::bson::doc;

#[macro_use]
extern crate log;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// configuration file path.
    #[clap(short, long, default_value = "config.toml")]
    conf: String,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opts: Opts = Opts::parse();
    let conf: SyncerConfig = toml::from_slice(&fs::read(opts.conf)?)?;
    let conn: DbConnection = DbConnection::connect(&conf).await?;
    if let Err(e) = conn.check_permission().await {
        error!("Check permission failed, error message: {:?}", e);
        std::process::exit(1);
    }

    // read config and try to connect to relative database first.
    Ok(())
}
