use clap::Clap;
use mongo_sync::OplogSyncer;
use mongo_sync::SyncerConfigV2 as SyncerConfig;
use tracing::info;

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// configuration file path.
    #[clap(short, long, default_value = "config.toml")]
    conf: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let collector = tracing_subscriber::fmt().finish();

    tracing::subscriber::set_global_default(collector).expect("setting tracing default failed");

    let opts: Opts = Opts::parse();
    let data = std::fs::read(opts.conf).unwrap();
    let conf: SyncerConfig = toml::from_slice(&data).unwrap();

    // create a new thread to handle for oplog sync.
    let oplog_syncer = OplogSyncer::new(conf.get_src_url(), conf.get_oplog_storage_url())?;
    info!(
        source_oplog_uri = conf.get_src_url(),
        target_oplog_storage_url = conf.get_oplog_storage_url(),
        "Begin to sync oplog. "
    );
    oplog_syncer.sync_forever()?;

    // iterate through syncer configuration, and start new mongo_sync via command line.
    Ok(())
}
