use crate::influx::storage;
use anyhow::{Context, Result};
use influxdb::integrations::serde_integration::DatabaseQueryResult;
use influxdb::{Client, Query, ReadQuery, Timestamp};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use crate::query;

//TODO add option to sync using show series
// which should be disabled for influxdb with
// a huge number of timeseries
// it probably can be an option like "or"
// or we fetch fields, tag and tag values OR we use show series
// of course the tables created would differ so users must be aware


//TODO accept a sync command which forces a sync even without been during the duration

pub struct Sync {
    //TODO rename to something like influx_database
    pub database: String,
    pub url: String,
    pub path: String,
    //TODO this duration will be used to send sleep/send messages
    // to a channel that will start the sync process
    pub interval: Duration,
    pub db: query::engine::DB,
    client: Arc<Client>,
}

impl Sync {
    pub fn new(database: String, url: String, path: String, interval: Duration) -> Self {
        let client = Arc::new(Client::new(&url, &database));
        Self {
            database,
            url,
            path: path.clone(),
            interval,
            client,
            db: query::engine::DB::new(path.into())
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
            let path_to_save = self.path.clone();
            let client = self.client.clone();

            join_set
                .spawn(async move { sync_measurement(client, measurement, path_to_save).await });
        }

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(measurement) => {
                    println!("task succeeded measurement {measurement}");
                    self.db.register(measurement).await.context("could not register measurement")?;
                } ,
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

async fn sync_measurement(client: Arc<Client>, measurement: String, save_at: String) -> String {
    let measurement = measurement.replace("\"", "");
    let path: PathBuf = save_at.into();
    let tag_file_path = path.join(format!("{measurement}_tags.parquet"));
    let field_file_path = path.join(format!("{measurement}_fields.parquet"));
    let tag_names = sync_tag_names(client.as_ref(), &measurement).await.unwrap();
    let tag_values = sync_tag_values(client.as_ref(), &measurement, &tag_names)
        .await
        .unwrap();

    storage::write_tag_file(&tag_file_path, &tag_values).unwrap();
    let fields = sync_fields(client.as_ref(), &measurement).await.unwrap();
    storage::write_field_file(&field_file_path, &fields).unwrap();

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
            .json_query(ReadQuery::new(format!(
                "show tag values from {measurement} with key = {tag}"
            )))
            .await
            .unwrap();


        let mut tag_vector = vec![];

        let tag_values = match get_values_from_query(&result) {
            None => continue,
            Some(r) => r,
        };



        for tag_value in tag_values {
            let tag_value = tag_value.as_array().unwrap().get(1).unwrap().to_string();
            tag_vector.push(tag_value);
        }

        final_result.insert(tag.to_string(), tag_vector);
    }

    Ok(final_result)
}

async fn sync_fields(client: &Client, measurement: &str) -> Result<Vec<String>> {
    let result = client
        .json_query(ReadQuery::new(format!(
            "show field keys from {measurement}"
        )))
        .await
        .unwrap();

    let mut fields_vector = vec![];

    let fields = match get_values_from_query(&result) {
        None => return Ok(fields_vector),
        Some(r) => r,
    };
    for field in fields {
        let field = field.as_array().unwrap().get(0).unwrap().to_string();
        fields_vector.push(field);
    }

    Ok(fields_vector)
}

//TODO integration tests
