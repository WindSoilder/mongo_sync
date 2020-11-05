use clap::Clap;
use mongo_sync::SyncerConfig;
use mongodb::Client;
use std::fs;

use mongodb::bson::doc;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// configuration file path.
    #[clap(short, long, default_value = "config.toml")]
    conf: String,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts: Opts = Opts::parse();
    let client = Client::with_uri_str("mongodb://localhost").await?;
    let coll = client.database("aa").collection("bb");

    coll.insert_one(doc! {"x": 1}, None).await.unwrap();
    println!("Config file path: {:?}", opts);
    Ok(())
}
