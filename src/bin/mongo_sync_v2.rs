use clap::Clap;
use mongo_sync::OplogSyncer;
use mongo_sync::SyncerConfig;

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

    let oplog_syncer: OplogSyncer =
        OplogSyncer::new(conf.get_src_url(), conf.get_oplog_storage_uri())?;
    oplog_syncer.sync_forever()?;
    // iterate through syncer configuration, and start new mongo_sync via command line.
    Ok(())
}
