use mongodb::bson::doc;
use mongodb::options::ListDatabasesOptions;
use mongodb::Client;

#[async_std::main]
async fn main() {
    //let cli = Client::with_uri_str("mongodb://root:Ricemap123@192.168.10.14:27017,192.168.10.15:27017,192.168.10.16:27017/?authSource=admin").await.unwrap();
    let cli = Client::with_uri_str("mongodb://root:Ricemap123@localhost:27017/?authSource=admin")
        .await
        .unwrap();
    cli.database("c")
        .list_collection_names(doc! {})
        .await
        .unwrap();
}
