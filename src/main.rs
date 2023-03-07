use std::time::Duration;

mod influx;

#[tokio::main]
async fn main() {
    let sync = influx::sync::Sync::new(
        "test".into(),
        "http://localhost:8086".into(),
        "nothing".into(),
        Duration::from_secs(60),
    );
    sync.sync().await;
}
