use arrow::array::RecordBatch;
use reqwest::Client;

use crate::{
    api::{IngestData, IngestFormat, do_ingest, do_submit_statement},
    config::Config,
    error::Error,
};

mod api;
mod codec;
mod config;
mod error;

/// A connection to a ScopeDB instance.
pub struct Connection {
    config: Config,
    client: Client,
}

impl Connection {
    /// Connect to a ScopeDB instance. The endpoint is the base URL of the instance.
    pub fn connect(endpoint: &str) -> Self {
        Self {
            config: Config {
                endpoint: endpoint.to_string(),
            },
            client: Client::new(),
        }
    }

    /// Submit query and return the result as Arrow record batches.
    ///
    /// # Example
    /// ```ignore
    /// let conn = Connection::connect("http://localhost:6543");
    /// let result = conn.query("select 1").await.unwrap();
    /// ```
    pub async fn query(&self, statement: &str) -> Result<Vec<RecordBatch>, Error> {
        // TODO: support asynchronous queries
        let resp = do_submit_statement(
            &self.client,
            &self.config.endpoint,
            statement,
            api::ResultFormat::ArrowJson,
        )
        .await?;

        if resp.status != api::StatementStatus::Finished {
            return Err(Error::Internal("statement not finished".to_string()));
        }

        let result = if let Some(rs) = resp.result_set {
            codec::decode_arrow(&rs.rows)?
        } else {
            return Err(Error::Internal("no result set".to_string()));
        };

        Ok(result)
    }

    /// Insert record batches into a table.
    ///
    /// # Example
    /// ```ignore
    /// let conn = Connection::connect("http://localhost:6543");
    /// conn.insert("database", "schema", "table", &[record_batch]).await.unwrap();
    /// ```
    pub async fn insert(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        data: &[RecordBatch],
    ) -> Result<(), Error> {
        let data = codec::encode_arrow(data)?;
        let ingest_data = IngestData {
            format: IngestFormat::Arrow,
            rows: data,
        };
        let statement = format!("insert into {database}.{schema}.{table}");
        do_ingest(&self.client, &self.config.endpoint, ingest_data, &statement).await?;

        Ok(())
    }
}
