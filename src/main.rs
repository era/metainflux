use std::time::Duration;

mod influx;
mod query;
//TODO flight protocol

#[tokio::main]
async fn main() {
    let path = "/tmp/meta/";
    let sync = influx::sync::Sync::new(
        "test".into(),
        "http://localhost:8086".into(),
        path.into(),
        Duration::from_secs(60),
    );
    sync.sync().await.unwrap();
    let result = sync.db.sql("select * from treasures_tags").await.unwrap();
    println!("{:?}", result);
}
