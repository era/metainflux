use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::listing::ListingTableConfig;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

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

    pub async fn register(&self) -> Result<()> {
        self.register_tags().await?;
        self.register_fields().await?;
        Ok(())
    }

    /// example usage
    /// register_files("tags", "/tmp/tags/*.parquet")
    async fn register_files(&self, table_name: &str, file_path: &str) -> Result<()> {
        self.ctx
            .register_parquet(
                &table_name,
                self.path.join(file_path).to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .context("could not register tag table")
    }

    async fn register_listener(&self, table_name: &str, table_path: &str) -> Result<()> {
        let table_path = ListingTableUrl::parse(table_path).context("could not parse table url")?;

        // Create default parquet options
        let file_format = ParquetFormat::new();
        let listing_options =
            ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

        // Resolve the schema
        let resolved_schema = listing_options
            .infer_schema(&self.ctx.state(), &table_path)
            .await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config)?);

        // or registered as a named table:
        self.ctx.register_table(table_name, provider);

        Ok(())
    }

    async fn register_tags(&self) -> Result<()> {
        self.register_listener("tags", self.path.join("tags").to_str().unwrap())
            .await
            .context("could not register tag table")
    }

    async fn register_fields(&self) -> Result<()> {
        self.register_listener("fields", self.path.join("fields").to_str().unwrap())
            .await
            .context("could not register fields table")
    }

    //TODO register edit_distance function
    // https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udf
}

//TODO tests
