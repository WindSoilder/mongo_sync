use std::fs;
use mongo_sync::SyncerConfig;


fn main() {
    let mut content: String = String::new();
    let conf: SyncerConfig = toml::from_str(&fs::read_to_string("config.toml").unwrap()).unwrap();
}
