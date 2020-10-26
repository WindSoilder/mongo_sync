use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct SyncerConfig {
    pub src: Src,
    dst: Dst,
    sync: Sync,
    log: Log,
}

#[derive(Deserialize, Debug)]
pub struct Src {
    url: String,
}

#[derive(Deserialize, Debug)]
struct Dst {
    url: String,
}

#[derive(Deserialize, Debug)]
pub struct Sync {
    dbs: Vec<Db>
}

#[derive(Deserialize, Debug)]
pub struct Log {
    optime_path: String,
}

#[derive(Deserialize, Debug)]
pub struct Db {
    db: String,
    colls: Vec<String>,
}

