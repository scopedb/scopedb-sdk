// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultFormat {
    #[serde(rename = "arrow-json")]
    ArrowJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "finished")]
    Finished,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementEstimatedProgress {
    pub total_percentage: f64,
    pub nanos_from_submitted: i64,
    pub nanos_from_started: i64,
    pub nanos_to_finish: i64,
    #[serde(flatten)]
    pub details: StatementProgress,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct StatementProgress {
    pub total_stages: i64,
    pub total_partitions: i64,
    pub total_rows: i64,
    pub total_compressed_bytes: i64,
    pub total_uncompressed_bytes: i64,
    pub scanned_stages: i64,
    pub scanned_partitions: i64,
    pub scanned_rows: i64,
    pub scanned_compressed_bytes: i64,
    pub scanned_uncompressed_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    pub metadata: ResultSetMetadata,
    pub format: ResultFormat,
    pub rows: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetMetadata {
    pub fields: Vec<ResultSetField>,
    pub num_rows: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetField {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementRequest {
    pub statement: String,
    pub exec_timeout: Option<String>,
    pub format: ResultFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    pub progress: StatementEstimatedProgress,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set: Option<ResultSet>,
}

const DEFAULT_EXEC_TIMEOUT: &str = "30s";

pub async fn do_submit_statement(
    client: &reqwest::Client,
    endpoint: &str,
    statement: &str,
    format: ResultFormat,
) -> Result<StatementResponse, Error> {
    let req = StatementRequest {
        statement: statement.to_string(),
        exec_timeout: Some(DEFAULT_EXEC_TIMEOUT.to_string()),
        format,
    };

    let resp: StatementResponse = client
        .post(format!("{endpoint}/v1/statements"))
        .json(&req)
        .send()
        .await
        .map_err(|e| Error::Internal(format!("failed to submit statement: {e}")))?
        .json()
        .await
        .map_err(|e| Error::Internal(format!("failed to parse response: {e}")))?;

    Ok(resp)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IngestFormat {
    #[serde(rename = "arrow")]
    Arrow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestData {
    pub format: IngestFormat,
    pub rows: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub data: IngestData,
    pub statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub num_rows_inserted: i64,
    pub num_rows_updated: i64,
    pub num_rows_deleted: i64,
}

pub async fn do_ingest(
    client: &reqwest::Client,
    endpoint: &str,
    data: IngestData,
    statement: &str,
) -> Result<IngestResponse, Error> {
    let req = IngestRequest {
        data,
        statement: statement.to_string(),
    };

    let resp: IngestResponse = client
        .post(format!("{endpoint}/v1/ingest"))
        .json(&req)
        .send()
        .await
        .map_err(|e| Error::Internal(format!("failed to ingest: {e}")))?
        .json()
        .await
        .map_err(|e| Error::Internal(format!("failed to parse response: {e}")))?;

    Ok(resp)
}
