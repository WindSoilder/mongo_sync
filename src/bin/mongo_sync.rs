use clap::Clap;

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
    Ok(())
}
