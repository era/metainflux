use anyhow::{Context, Result};
use influxdb::integrations::serde_integration::DatabaseQueryResult;
use influxdb::{Client, Query, ReadQuery, Timestamp};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

pub struct Sync {
    pub database: String,
    pub url: String,
    pub path: String,
    pub interval: Duration,
    client: Arc<Client>,
}

impl Sync {
    pub fn new(database: String, url: String, path: String, interval: Duration) -> Self {
        let client = Arc::new(Client::new(&url, &database));
        Self {
            database,
            url,
            path,
            interval,
            client,
        }
    }
    pub async fn sync(&self) -> Result<()> {
        let measurements = self.show_measurements().await?;

        let measurements = match get_values_from_query(&measurements) {
            None => return Ok(()),
            Some(r) => r,
        };

        let mut join_set = JoinSet::new();

        for measurement in measurements {
            let measurement = measurement.as_array().unwrap().get(0).unwrap().to_string();
            let client = self.client.clone();

            join_set.spawn(async move { sync_measurement(client, measurement).await });
        }

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(measurement) => println!("task succeeded measurement {measurement}"),
                Err(e) => eprintln!("task failed {:?}", e),
            };
        }

        Ok(())
    }
    pub async fn show_measurements(&self) -> Result<DatabaseQueryResult> {
        self.client
            .json_query(ReadQuery::new("show measurements"))
            .await
            .context("could not query influxdb")
    }
}

// extract values from InfluxQL results
fn get_values_from_query(result: &DatabaseQueryResult) -> Option<&Vec<Value>> {
    // we always have one result, since we have done one query
    let result = result.results.get(0).unwrap().as_object().unwrap();

    // that result could be empty, so we need to check
    match result.get("series") {
        None => None,
        // if it's not empty, we must have at least one value
        Some(result) => result
            .as_array()
            .unwrap()
            .get(0) // we only have one series (measurements)
            .unwrap()
            .as_object()
            .unwrap()
            .get("values") // and we are only interested in the values key
            .unwrap()
            .as_array(),
    }
}

async fn sync_measurement(client: Arc<Client>, measurement: String) -> String {
    let tag_names = sync_tag_names(client.as_ref(), &measurement).await.unwrap();
    let tag_values = sync_tag_values(client.as_ref(), &measurement, &tag_names)
        .await
        .unwrap();
    //at this point write the tag values and tag names into a parquet file (format!("{measurement}_tags.parquet"))
    sync_fields(client.as_ref(), &measurement).await.unwrap();
    // at this point write the fields on a parquet file (format!("{measurement}_fields.parquet"))

    measurement
}

async fn sync_tag_names(client: &Client, measurement: &str) -> Result<Vec<String>> {
    let result = client
        .json_query(ReadQuery::new(format!("show tag keys from {measurement}")))
        .await
        .unwrap();

    let mut tag_vector = vec![];

    let tag_names = match get_values_from_query(&result) {
        None => return Ok(tag_vector),
        Some(r) => r,
    };
    for tag_name in tag_names {
        let tag_name = tag_name.as_array().unwrap().get(0).unwrap().to_string();
        tag_vector.push(tag_name);
    }

    Ok(tag_vector)
}
async fn sync_tag_values(
    client: &Client,
    measurement: &str,
    tag_names: &Vec<String>,
) -> Result<HashMap<String, Vec<String>>> {

    let mut final_result = HashMap::new();

    for tag in tag_names {
        let result = client
            .json_query(ReadQuery::new(format!("show tag values from {measurement} with key = {tag}")))
            .await
            .unwrap();

        let mut tag_vector = vec![];

        let tag_values = match get_values_from_query(&result) {
            None => continue,
            Some(r) => r,
        };

        for tag_value in tag_values {
            let tag_value = tag_value.as_array().unwrap().get(0).unwrap().to_string();
            tag_vector.push(tag_value);
        }

        final_result.insert(tag.to_string(), tag_vector);
    }

    Ok(final_result)
}

async fn sync_fields(client: &Client, measurement: &str) -> Result<()> {
    todo!()
}
