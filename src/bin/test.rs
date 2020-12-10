use mongodb::sync::Client;

fn main() {
    let source = Client::with_uri_str("mongodb://localhost/").unwrap();
    let target =
        Client::with_uri_str("mongodb://root:Ricemap123@192.168.10.67/?authSource=admin").unwrap();

    let coll_names = source.database("bb").list_collection_names(None).unwrap();
    let target_db = target.database("bb");
    let source_db = source.database("bb");
    let mut tasks = vec![];

    let time = std::time::SystemTime::now();
    for c in coll_names {
        let source_coll = source_db.collection(&c);
        let target_coll = target_db.collection(&c);

        tasks.push(std::thread::spawn(move || {
            println!("handle for {:?}", c);
            let mut buffer = vec![];

            target_coll.drop(None).unwrap();
            let mut cursor = source_coll.find(None, None).unwrap();
            while let Some(doc) = cursor.next() {
                buffer.push(doc.unwrap());
                if buffer.len() == 10000 {
                    let mut data_to_write = vec![];
                    std::mem::swap(&mut buffer, &mut data_to_write);
                    target_coll.insert_many(data_to_write, None).unwrap();
                }
            }
        }));
    }
    for t in tasks {
        t.join().unwrap();
    }
    println!("time: {}", time.elapsed().unwrap().as_secs_f32());
}
