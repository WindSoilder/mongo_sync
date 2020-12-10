use std::sync::Arc;
use crate::config::SyncerConfig;
use mongodb::sync::Client;


pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    source_conn: Client,
    target_conn: Client,
    config: Arc<SyncerConfig>,
}
