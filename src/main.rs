#![feature(async_closure)]

use std::thread;
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
    sync.setup_sync().unwrap();
    println!("going to sleep to wait first sync");
    thread::sleep(Duration::from_secs(10));
    let result = sync.query("select * from tags").await.unwrap();
    println!("{:?}", result);
}
