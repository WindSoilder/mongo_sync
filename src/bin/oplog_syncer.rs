use clap::Clap;
use mongo_sync::{OplogCleaner, OplogSyncer};
use std::path::Path;
use std::time::Duration;
use tracing::{error, info};

#[derive(Clap, Debug)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
struct Opts {
    /// source database uri, must be a mongodb cluster.
    #[clap(short, long)]
    src_uri: String,
    /// target oplog storage uri.
    #[clap(short, long)]
    oplog_storage_uri: String,
    /// log file path, if not specified, all log information will be output to stdout.
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

    let sleep_time = 10;
    let max_retry_times = 10;
    let mut retry_times = 0;

    // start up cleaner.
    info!("Starting oplog cleaner...");
    let storage_uri = opts.oplog_storage_uri.clone();
    std::thread::Builder::new()
        .name("oplog cleaner".to_string())
        .spawn(move || {
            let cleaner = OplogCleaner::new(storage_uri);
            let one_day_in_secs = 24 * 60 * 60;
            loop {
                let clean_cnt = cleaner.run_clean().unwrap();
                info!(%clean_cnt, "Cleaner thread: clean oplog done, going to sleep :-D");
                // sleep one day..
                std::thread::sleep(Duration::from_secs(one_day_in_secs));
            }
        })?;
    info!("Starting oplog cleaner complete...");

    loop {
        let oplog_syncer: OplogSyncer = OplogSyncer::new(&opts.src_uri, &opts.oplog_storage_uri)?;
        let res = oplog_syncer.sync_forever();
        if let Err(e) = res {
            error!(?e, "Sync oplog error occurred. ");
        }
        info!(sleep_time, "Wait for a while.....");
        std::thread::sleep(std::time::Duration::from_secs(sleep_time));
        if retry_times >= max_retry_times {
            info!(%retry_times, "No, retried several times, the server is still down, I'm leaving now.");
            break Ok(());
        }
        retry_times += 1;
    }
    // iterate through syncer configuration, and start new mongo_sync via command line.
}
