use clap::Clap;
use mongo_sync::SyncerConfig;
use std::fs;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// configuration file path.
    #[clap(short, long, default_value = "config.toml")]
    conf: String,
}

fn main() {
    let opts: Opts = Opts::parse();

    println!("Config file path: {:?}", opts);
}
