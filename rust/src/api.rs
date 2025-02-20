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
pub struct StatementProgress {
    pub total_percentage: f64,
    pub nanos_from_submitted: i64,
    pub nanos_from_started: i64,
    pub nanos_to_finish: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    pub metadata: ResultSetMetadata,
    pub rows: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetMetadata {
    pub fields: Vec<ResultSetField>,
    pub format: ResultFormat,
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
    pub wait_timeout: Option<String>,
    pub exec_timeout: Option<String>,
    pub format: ResultFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    pub result_set: Option<ResultSet>,
}

const DEFAULT_WAIT_TIMEOUT: &str = "30s";
const DEFAULT_EXEC_TIMEOUT: &str = "30s";

pub async fn do_submit_statement(
    client: &reqwest::Client,
    endpoint: &str,
    statement: &str,
    format: ResultFormat,
) -> Result<StatementResponse, Error> {
    let req = StatementRequest {
        statement: statement.to_string(),
        wait_timeout: Some(DEFAULT_WAIT_TIMEOUT.to_string()),
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

#[cfg(test)]
mod test {
    use insta::assert_json_snapshot;

    use super::*;

    #[test]
    fn test_serialization() {
        assert_json_snapshot!(
            StatementRequest {
                statement: "from t select 1".to_string(),
                wait_timeout: Some("30s".to_string()),
                exec_timeout: Some("30s".to_string()),
                format: ResultFormat::ArrowJson,
            },
            @r#"
        {
          "statement": "from t select 1",
          "wait_timeout": "30s",
          "exec_timeout": "30s",
          "format": "arrow-json"
        }
        "#,
        );
        assert_json_snapshot!(
            StatementResponse {
                statement_id: "1".to_string(),
                status: StatementStatus::Pending,
                result_set: None,
            },
            @r#"
        {
          "statement_id": "1",
          "status": "pending",
          "result_set": null
        }
        "#,
        );
    }
}
