use clap::Clap;
use mongo_sync::SyncerConfigV2 as SyncerConfig;
use mongo_sync::OplogSyncer;


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

    // create a new thread to handle for oplog sync.
    let oplog_syncer = OplogSyncer::new(conf.get_src_url(), conf.get_oplog_storage_url())?;
    oplog_syncer.sync_forever()?;

    // iterate through syncer configuration, and start new mongo_sync via command line.
    Ok(())
}
