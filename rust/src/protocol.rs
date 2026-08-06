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
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

use jiff::SignedDuration;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::ResultSet;

/// The commit outcome reported by the table append API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppendState {
    /// Every row in the request was committed.
    Committed,
    /// The request was rejected before any row was committed.
    Rejected,
    /// The server cannot determine whether the request committed.
    Unknown,
}

impl fmt::Display for AppendState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Committed => f.write_str("committed"),
            Self::Rejected => f.write_str("rejected"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

/// The result of a committed table append.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowsResult {
    pub append_state: AppendState,
    pub num_rows_inserted: i64,
}

/// A validation error for one row in an append request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowError {
    pub row_index: u64,
    pub column: String,
    pub message: String,
}

/// Structured failure details returned by the table append API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendErrorDetails {
    pub append_state: AppendState,
    #[serde(default)]
    pub row_errors: Vec<AppendRowError>,
    #[serde(default)]
    pub row_errors_truncated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AppendRowsErrorPayload {
    #[serde(rename = "message")]
    pub _message: String,
    #[serde(flatten)]
    pub details: AppendErrorDetails,
}

/// Pagination options for catalog list requests.
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogListOptions {
    /// Number of resources to return. The server accepts values from 1 to 1000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
    /// Opaque token returned as `next_page_token` by the previous page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl CatalogListOptions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self
            .page_size
            .is_some_and(|size| !(1..=1000).contains(&size))
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "catalog page_size must be an integer from 1 to 1000",
            ));
        }
        Ok(())
    }
}

/// One page of catalog resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage<T> {
    pub items: Vec<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseResource {
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaResource {
    pub database: String,
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableResourceSummary {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDistinctSpec {
    pub on: Vec<String>,
    pub by: Vec<String>,
}

/// The public specification of a table resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSpec {
    pub columns: Vec<TableColumnSpec>,
    pub partition_by: Vec<String>,
    pub cluster_by: Vec<String>,
    pub distinct_on: TableDistinctSpec,
    pub data_retention_days: Option<i32>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableResource {
    pub database: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<TableColumnSpec>,
    pub partition_by: Vec<String>,
    pub cluster_by: Vec<String>,
    pub distinct_on: TableDistinctSpec,
    pub data_retention_days: Option<i32>,
    pub comment: Option<String>,
}

impl TableResource {
    /// Returns the reusable table specification without its catalog identity.
    pub fn spec(&self) -> TableSpec {
        TableSpec {
            columns: self.columns.clone(),
            partition_by: self.partition_by.clone(),
            cluster_by: self.cluster_by.clone(),
            distinct_on: self.distinct_on.clone(),
            data_retention_days: self.data_retention_days,
            comment: self.comment.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Response<T> {
    Success(T),
    Failed(ErrorStatus),
}

impl<T: DeserializeOwned> Response<T> {
    pub async fn from_http_response(r: reqwest::Response) -> Result<Self, Error> {
        let code = r.status();
        let headers = r.headers().clone();
        let payload = r.bytes().await.map_err(|err| {
            apply_response_metadata(
                Error::new(ErrorKind::Unexpected, "failed to read response body").set_source(err),
                code,
                &headers,
            )
        })?;
        if code.is_success() {
            let result = serde_json::from_slice(&payload).map_err(|err| {
                apply_response_metadata(
                    Error::new(ErrorKind::Unexpected, "failed to decode response body")
                        .set_source(err),
                    code,
                    &headers,
                )
            })?;
            return Ok(Response::Success(result));
        }

        Ok(Response::Failed(ErrorStatus::from_http_parts(
            code, &headers, &payload,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct ErrorStatus {
    code: StatusCode,
    message: String,
    request_id: Option<String>,
    retry_after: Option<Duration>,
    retryable: Option<bool>,
}

impl ErrorStatus {
    pub(crate) fn from_http_parts(code: StatusCode, headers: &HeaderMap, payload: &[u8]) -> Self {
        let parsed = parse_error_payload(payload);
        Self {
            code,
            message: parsed
                .as_ref()
                .map(|parsed| parsed.message.clone())
                .unwrap_or_else(|| String::from_utf8_lossy(payload).into_owned()),
            request_id: parsed
                .as_ref()
                .and_then(|parsed| parsed.request_id.clone())
                .or_else(|| header_string(headers, "x-request-id")),
            retry_after: parse_retry_after(headers, SystemTime::now()),
            retryable: parsed.and_then(|parsed| parsed.retryable),
        }
    }

    pub(crate) fn into_error(self, kind: ErrorKind) -> Error {
        let mut error = Error::new(kind, self.message).set_http_status(self.code);
        if let Some(request_id) = self.request_id {
            error = error.set_request_id(request_id);
        }
        if let Some(retry_after) = self.retry_after {
            error = error.set_retry_after(retry_after);
        }
        let retryable = self.retryable.unwrap_or_else(|| {
            matches!(
                self.code,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            ) || self.code.is_server_error()
        });
        if retryable {
            error.set_temporary()
        } else {
            error.set_permanent()
        }
    }
}

#[derive(Debug)]
struct ParsedErrorPayload {
    message: String,
    request_id: Option<String>,
    retryable: Option<bool>,
}

fn parse_error_payload(payload: &[u8]) -> Option<ParsedErrorPayload> {
    let payload = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
    let object = payload.as_object()?;
    if let Some(message) = object.get("message").and_then(serde_json::Value::as_str) {
        return Some(ParsedErrorPayload {
            message: message.to_string(),
            request_id: object
                .get("request_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            retryable: object.get("retryable").and_then(serde_json::Value::as_bool),
        });
    }

    let nested = object.get("error")?.as_object()?;
    Some(ParsedErrorPayload {
        message: nested.get("message")?.as_str()?.to_string(),
        request_id: object
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        retryable: nested.get("retryable").and_then(serde_json::Value::as_bool),
    })
}

pub(crate) fn apply_response_metadata(
    mut error: Error,
    status: StatusCode,
    headers: &HeaderMap,
) -> Error {
    error = error.set_http_status(status);
    if let Some(request_id) = header_string(headers, "x-request-id") {
        error = error.set_request_id(request_id);
    }
    if let Some(retry_after) = parse_retry_after(headers, SystemTime::now()) {
        error = error.set_retry_after(retry_after);
    }
    error
}

fn header_string(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn parse_retry_after(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} ({}): {}",
            self.code.canonical_reason().unwrap_or("Unknown"),
            self.code.as_u16(),
            self.message,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format")]
pub(crate) enum IngestData {
    #[serde(rename = "json")]
    Json { rows: String },
}

impl IngestData {
    pub fn format(&self) -> &'static str {
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
pub struct IngestResult {
    pub num_rows_inserted: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ResultFormat {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec_timeout: Option<SignedDuration>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_parallelism: Option<usize>,
    #[serde(flatten)]
    pub params: StatementRequestParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementCancelResult {
    pub statement_id: Uuid,
    pub status: String,
    pub message: String,
    pub created_at: jiff::Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum StatementStatus {
    #[serde(rename = "pending")]
    Pending(StatementStatusPending),
    #[serde(rename = "running")]
    Running(StatementStatusRunning),
    #[serde(rename = "finished")]
    Finished(StatementStatusFinished),
    #[serde(rename = "failed")]
    Failed(StatementStatusFailed),
    #[serde(rename = "cancelled")]
    Cancelled(StatementStatusCancelled),
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusPending {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusRunning {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusFinished {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,

    result_set: StatementResultSet,
}

impl StatementStatusFinished {
    pub fn result_set(&self) -> ResultSet {
        ResultSet::from_statement_result_set(self.result_set.clone())
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusFailed {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
    pub message: String,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatusCancelled {
    pub statement_id: Uuid,
    pub created_at: jiff::Timestamp,
    pub progress: StatementEstimatedProgress,
    pub message: String,
}

impl StatementStatus {
    pub fn statement_id(&self) -> Uuid {
        match self {
            StatementStatus::Pending(s) => s.statement_id,
            StatementStatus::Running(s) => s.statement_id,
            StatementStatus::Finished(s) => s.statement_id,
            StatementStatus::Failed(s) => s.statement_id,
            StatementStatus::Cancelled(s) => s.statement_id,
        }
    }

    pub fn created_at(&self) -> jiff::Timestamp {
        match self {
            StatementStatus::Pending(s) => s.created_at,
            StatementStatus::Running(s) => s.created_at,
            StatementStatus::Finished(s) => s.created_at,
            StatementStatus::Failed(s) => s.created_at,
            StatementStatus::Cancelled(s) => s.created_at,
        }
    }

    pub fn progress(&self) -> &StatementEstimatedProgress {
        match self {
            StatementStatus::Pending(s) => &s.progress,
            StatementStatus::Running(s) => &s.progress,
            StatementStatus::Finished(s) => &s.progress,
            StatementStatus::Failed(s) => &s.progress,
            StatementStatus::Cancelled(s) => &s.progress,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, StatementStatus::Finished(..))
    }

    pub fn is_terminated(&self) -> bool {
        matches!(
            self,
            StatementStatus::Finished(..)
                | StatementStatus::Failed(..)
                | StatementStatus::Cancelled(..)
        )
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
    pub skipped_partitions: i64,
    pub skipped_rows: i64,
    pub skipped_compressed_bytes: i64,
    pub skipped_uncompressed_bytes: i64,
}

impl StatementProgress {
    pub fn total_percentage(&self) -> f64 {
        let scan_progress = if self.total_rows == 0 {
            0.0
        } else {
            (self.scanned_rows + self.skipped_rows) as f64 / self.total_rows as f64 * 100.0
        };

        let stage_progress = if self.total_stages == 0 {
            0.0
        } else {
            self.scanned_stages as f64 / self.total_stages as f64 * 100.0
        };

        scan_progress.max(stage_progress)
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
pub enum DataType {
    #[serde(rename = "int")]
    Int,
    #[serde(alias = "u_int")] // backward compatibility
    #[serde(rename = "uint")]
    UInt,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "interval")]
    Interval,
    #[serde(rename = "array")]
    Array,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "null")]
    Null,
}

impl FromStr for DataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int" => Ok(Self::Int),
            "uint" | "u_int" => Ok(Self::UInt),
            "float" => Ok(Self::Float),
            "binary" => Ok(Self::Binary),
            "string" => Ok(Self::String),
            "boolean" => Ok(Self::Boolean),
            "timestamp" => Ok(Self::Timestamp),
            "interval" => Ok(Self::Interval),
            "array" => Ok(Self::Array),
            "object" => Ok(Self::Object),
            "any" => Ok(Self::Any),
            "null" => Ok(Self::Null),
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                format!("unrecognized data type: {s}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::SystemTime;

    use reqwest::header::HeaderMap;
    use reqwest::header::HeaderValue;

    use super::AppendRowsErrorPayload;
    use super::AppendState;
    use super::CatalogListOptions;
    use super::DataType;
    use super::ErrorStatus;
    use super::TableResource;
    use super::parse_error_payload;
    use super::parse_retry_after;
    use crate::ErrorKind;

    #[test]
    fn append_error_details_use_wire_defaults() {
        let payload: AppendRowsErrorPayload =
            serde_json::from_str(r#"{"message":"not committed","append_state":"rejected"}"#)
                .unwrap();

        assert_eq!(payload.details.append_state, AppendState::Rejected);
        assert!(payload.details.row_errors.is_empty());
        assert!(!payload.details.row_errors_truncated);
    }

    #[test]
    fn http_error_message_supports_direct_and_nested_payloads() {
        assert_eq!(
            parse_error_payload(br#"{"message":"direct failure","error":"details"}"#)
                .unwrap()
                .message,
            "direct failure"
        );
        assert_eq!(
            parse_error_payload(br#"{"error":{"message":"nested failure"}}"#)
                .unwrap()
                .message,
            "nested failure"
        );
    }

    #[test]
    fn http_error_metadata_prefers_payload_and_honors_retryable() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("header-request"));
        headers.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("7"));

        let error = ErrorStatus::from_http_parts(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            &headers,
            br#"{"message":"busy","request_id":"body-request","retryable":false}"#,
        )
        .into_error(ErrorKind::Unexpected);

        assert_eq!(error.message(), "busy");
        assert_eq!(
            error.http_status(),
            Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(error.request_id(), Some("body-request"));
        assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
        assert!(!error.is_retryable());

        let nested = ErrorStatus::from_http_parts(
            reqwest::StatusCode::BAD_REQUEST,
            &HeaderMap::new(),
            br#"{"error":{"message":"try later","retryable":true}}"#,
        )
        .into_error(ErrorKind::Unexpected);
        assert_eq!(nested.message(), "try later");
        assert!(nested.is_retryable());
    }

    #[test]
    fn retry_after_accepts_http_dates() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:10 GMT"),
        );
        assert_eq!(
            parse_retry_after(&headers, SystemTime::UNIX_EPOCH),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn catalog_page_size_is_validated_locally() {
        let options = CatalogListOptions {
            page_size: Some(1001),
            page_token: None,
        };
        let error = options.validate().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn table_resource_deserializes_flattened_spec() {
        let table: TableResource = serde_json::from_str(
            r#"{
                "database":"scopedb",
                "schema":"public",
                "name":"events",
                "columns":[{"name":"message","data_type":"string","comment":null}],
                "partition_by":["date(ts)"],
                "cluster_by":["service"],
                "distinct_on":{"on":[],"by":[]},
                "data_retention_days":30,
                "comment":"application events"
            }"#,
        )
        .unwrap();

        assert_eq!(table.name, "events");
        assert_eq!(table.columns[0].data_type, DataType::String);
        assert_eq!(table.data_retention_days, Some(30));
    }
}
