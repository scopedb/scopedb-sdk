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

use std::fmt;

use exn::ResultExt;
use jiff::SignedDuration;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::Error;

#[derive(Debug, Clone)]
pub enum Response<T> {
    Success(T),
    Failed(ErrorStatus),
}

impl<T: DeserializeOwned> Response<T> {
    pub async fn from_http_response(r: reqwest::Response) -> exn::Result<Self, Error> {
        let make_error = || Error("failed to make response".to_string());

        let code = r.status();
        if code.is_success() {
            let result = r.json().await.or_raise(make_error)?;
            return Ok(Response::Success(result));
        }

        #[derive(Deserialize)]
        struct ErrorMessage {
            message: String,
        }

        let payload = r.bytes().await.or_raise(make_error)?;
        if let Ok(ErrorMessage { message }) = serde_json::from_slice::<ErrorMessage>(&payload) {
            Ok(Response::Failed(ErrorStatus { code, message }))
        } else {
            let message = String::from_utf8_lossy(&payload).into_owned();
            Ok(Response::Failed(ErrorStatus { code, message }))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorStatus {
    code: StatusCode,
    message: String,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?} ({}): {}",
            self.code.canonical_reason(),
            self.code.as_u16(),
            self.message,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format")]
pub enum IngestData {
    #[serde(rename = "json")]
    Json { rows: String },
}

impl IngestData {
    pub fn format(&self) -> &str {
        match self {
            Self::Json { .. } => "json",
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestType {
    #[default]
    #[serde(rename = "committed")]
    Committed,
    #[serde(rename = "buffered")]
    Buffered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    #[serde(default)]
    #[serde(rename = "type")]
    pub ty: IngestType,
    pub data: IngestData,
    pub statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub num_rows_inserted: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultFormat {
    #[serde(rename = "json")]
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementRequestParams {
    pub format: ResultFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementRequest {
    pub statement: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_id: Option<Uuid>,
    #[serde(default)]
    pub exec_timeout: Option<SignedDuration>,
    #[serde(flatten)]
    pub params: StatementRequestParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementCancelResponse {
    pub statement_id: Uuid,
    pub status: String,
    pub message: String,
    pub created_at: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum StatementResponse {
    #[serde(rename = "pending")]
    Pending {
        statement_id: Uuid,
        created_at: jiff::Timestamp,
        progress: StatementEstimatedProgress,
    },
    #[serde(rename = "running")]
    Running {
        statement_id: Uuid,
        created_at: jiff::Timestamp,
        progress: StatementEstimatedProgress,
    },
    #[serde(rename = "finished")]
    Finished {
        statement_id: Uuid,
        created_at: jiff::Timestamp,
        progress: StatementEstimatedProgress,
        result_set: StatementResultSet,
    },
    #[serde(rename = "failed")]
    Failed {
        statement_id: Uuid,
        created_at: jiff::Timestamp,
        progress: StatementEstimatedProgress,
        message: String,
    },
    #[serde(rename = "cancelled")]
    Cancelled {
        statement_id: Uuid,
        created_at: jiff::Timestamp,
        progress: StatementEstimatedProgress,
        message: String,
    },
}

impl StatementResponse {
    pub fn statement_id(&self) -> Uuid {
        match self {
            Self::Pending { statement_id, .. }
            | Self::Running { statement_id, .. }
            | Self::Finished { statement_id, .. }
            | Self::Failed { statement_id, .. }
            | Self::Cancelled { statement_id, .. } => *statement_id,
        }
    }

    pub fn status(&self) -> &str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Running { .. } => "running",
            Self::Finished { .. } => "finished",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    pub fn is_terminated(&self) -> bool {
        matches!(
            self,
            Self::Finished { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }

    pub fn progress(&self) -> &StatementEstimatedProgress {
        match self {
            Self::Pending { progress, .. }
            | Self::Running { progress, .. }
            | Self::Finished { progress, .. }
            | Self::Failed { progress, .. }
            | Self::Cancelled { progress, .. } => progress,
        }
    }

    pub fn result_set(&self) -> Option<&StatementResultSet> {
        match self {
            StatementResponse::Finished { result_set, .. } => Some(result_set),
            StatementResponse::Pending { .. }
            | StatementResponse::Running { .. }
            | StatementResponse::Failed { .. }
            | StatementResponse::Cancelled { .. } => None,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct StatementEstimatedProgress {
    /// Total progress in percentage: `[0.0, 100.0]`.
    pub total_percentage: f64,
    /// Duration in nanoseconds since the statement is submitted.
    pub nanos_from_submitted: i64,
    /// Duration in nanoseconds since the statement is started.
    pub nanos_from_started: i64,
    #[serde(flatten)]
    pub details: StatementProgress,
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

impl StatementProgress {
    pub fn total_percentage(&self) -> f64 {
        if self.total_stages == 0 {
            0.0
        } else {
            self.scanned_stages as f64 / self.total_stages as f64 * 100.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResultSet {
    pub metadata: ResultSetMetadata,
    #[serde(flatten)]
    pub data: ResultSetData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format")]
pub enum ResultSetData {
    #[serde(rename = "json")]
    Json { rows: Vec<Vec<Option<String>>> },
}

impl ResultSetData {
    pub fn format(&self) -> ResultFormat {
        match self {
            Self::Json { .. } => ResultFormat::Json,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSetMetadata {
    pub fields: Vec<FieldMetadata>,
    pub num_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Int,
    UInt,
    Float,
    Binary,
    String,
    Boolean,
    Timestamp,
    Interval,
    Array,
    Object,
    Any,
    Null,
}
