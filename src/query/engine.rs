use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::path::PathBuf;

pub struct DB {
    path: PathBuf,
    ctx: SessionContext,
}

impl DB {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            ctx: SessionContext::new(),
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // create a plan to run a SQL query
        let df = self.ctx.sql(sql).await?;
        // execute and print results
        // df.show().await?;
        df.collect().await.context("error while executing sql")
    }

    pub async fn register(&self, measurement: String) -> Result<()> {
        self.register_tags(&measurement).await?;
        self.register_fields(&measurement).await?;
        Ok(())
    }
    async fn register_tags(&self, measurement: &str) -> Result<()> {
        let measurement = measurement.replace("\"", "");
        let table_name = format!("{measurement}_tags");
        let file_path = format!("{measurement}_tags.parquet");
        self.ctx
            .register_parquet(
                &table_name,
                self.path.join(file_path).to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .context("could not register tag table")
    }

    async fn register_fields(&self, measurement: &str) -> Result<()> {
        let measurement = measurement.replace("\"", "");
        let table_name = format!("{measurement}_fields");
        let file_path = format!("{measurement}_fields.parquet");
        self.ctx
            .register_parquet(
                &table_name,
                self.path.join(file_path).to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .context("could not register fields table")
    }

    //TODO register edit_distance function
    // https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udf
}


//TODO tests