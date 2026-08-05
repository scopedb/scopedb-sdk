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

use fastrace_reqwest::traceparent_headers;
use reqwest::IntoUrl;
use reqwest::StatusCode;
use reqwest::Url;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::IngestStreamBuilder;
use crate::Statement;
use crate::Table;
use crate::protocol::AppendErrorDetails;
use crate::protocol::AppendRowsErrorPayload;
use crate::protocol::AppendRowsResult;
use crate::protocol::AppendState;
use crate::protocol::CatalogListOptions;
use crate::protocol::CatalogPage;
use crate::protocol::DatabaseResource;
use crate::protocol::IngestData;
use crate::protocol::IngestRequest;
use crate::protocol::IngestResult;
use crate::protocol::IngestType;
use crate::protocol::Response;
use crate::protocol::ResultFormat;
use crate::protocol::SchemaResource;
use crate::protocol::StatementCancelResult;
use crate::protocol::StatementRequest;
use crate::protocol::StatementRequestParams;
use crate::protocol::StatementStatus;
use crate::protocol::TableResource;
use crate::protocol::TableResourceSummary;
use crate::statement::StatementHandle;

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn new<E: IntoUrl>(endpoint: E, client: reqwest::Client) -> Result<Self, Error> {
        match endpoint.into_url() {
            Ok(mut endpoint) => {
                if !endpoint.path().ends_with('/') {
                    let path = format!("{}/", endpoint.path());
                    endpoint.set_path(&path);
                }
                Ok(Self { endpoint, client })
            }
            Err(err) => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "failed to parse endpoint".to_string(),
            )
            .set_source(err)),
        }
    }

    pub fn statement(&self, statement: String) -> Statement {
        Statement::new(self.clone(), statement)
    }

    pub fn statement_handle(&self, statement_id: Uuid) -> StatementHandle {
        StatementHandle::new(self.clone(), statement_id, ResultFormat::Json)
    }

    pub fn table(&self, table: impl Into<String>) -> Table {
        Table::new(self.clone(), table.into())
    }

    pub fn ingest_stream(&self, statement: impl Into<String>) -> IngestStreamBuilder {
        IngestStreamBuilder::new(self.clone(), statement.into())
    }

    pub async fn health_check(&self) -> Result<(), Error> {
        let url = self.make_url("v1/health")?;
        self.client.get(url).send().await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to send health check request".to_string(),
            )
            .set_source(err)
        })?;
        Ok(())
    }

    pub async fn list_databases(
        &self,
        options: CatalogListOptions,
    ) -> Result<CatalogPage<DatabaseResource>, Error> {
        self.fetch_catalog(&["databases"], Some(&options), "failed to list databases")
            .await
    }

    pub async fn fetch_database(&self, database: &str) -> Result<DatabaseResource, Error> {
        self.fetch_catalog(&["databases", database], None, "failed to fetch database")
            .await
    }

    pub async fn list_schemas(
        &self,
        database: &str,
        options: CatalogListOptions,
    ) -> Result<CatalogPage<SchemaResource>, Error> {
        self.fetch_catalog(
            &["databases", database, "schemas"],
            Some(&options),
            "failed to list schemas",
        )
        .await
    }

    pub async fn fetch_schema(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<SchemaResource, Error> {
        self.fetch_catalog(
            &["databases", database, "schemas", schema],
            None,
            "failed to fetch schema",
        )
        .await
    }

    pub async fn list_tables(
        &self,
        database: &str,
        schema: &str,
        options: CatalogListOptions,
    ) -> Result<CatalogPage<TableResourceSummary>, Error> {
        self.fetch_catalog(
            &["databases", database, "schemas", schema, "tables"],
            Some(&options),
            "failed to list tables",
        )
        .await
    }

    pub async fn fetch_table(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableResource, Error> {
        self.fetch_catalog(
            &["databases", database, "schemas", schema, "tables", table],
            None,
            "failed to fetch table",
        )
        .await
    }

    /// Appends newline-delimited JSON rows to a table.
    pub async fn append_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        ndjson: impl Into<String>,
    ) -> Result<AppendRowsResult, Error> {
        let ndjson = ndjson.into();
        let expected_rows = ndjson
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        let url = self.make_resource_url(&[
            "databases",
            database,
            "schemas",
            schema,
            "tables",
            table,
            "rows",
        ])?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .map_err(|err| {
                append_unknown_error("failed to send table append request").set_source(err)
            })?;

        let status = response.status();
        let payload = response.bytes().await.map_err(|err| {
            append_unknown_error("failed to read table append response").set_source(err)
        })?;
        let result = decode_append_response(status, &payload)?;
        if result.num_rows_inserted != expected_rows as i64 {
            return Err(append_unknown_error(format!(
                "table append response reported {} inserted rows for a {expected_rows}-row request",
                result.num_rows_inserted
            )));
        }
        Ok(result)
    }

    pub async fn insert(&self, rows: String, transform: String) -> Result<IngestResult, Error> {
        match self
            .ingest(IngestRequest {
                ty: IngestType::Committed,
                data: IngestData::Json { rows },
                statement: transform,
            })
            .await?
        {
            Response::Success(result) => Ok(result),
            Response::Failed(err) => Err(map_failed_response(
                err,
                "failed to insert data".to_string(),
            )),
        }
    }
}

impl Client {
    async fn fetch_catalog<T: DeserializeOwned>(
        &self,
        segments: &[&str],
        options: Option<&CatalogListOptions>,
        operation: &'static str,
    ) -> Result<T, Error> {
        let url = self.make_resource_url(segments)?;
        let mut request = self.client.get(url).headers(traceparent_headers());
        if let Some(options) = options {
            request = request.query(options);
        }
        let response = request.send().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, operation)
                .set_source(err)
                .set_temporary()
        })?;
        match Response::from_http_response(response).await? {
            Response::Success(result) => Ok(result),
            Response::Failed(err) => Err(map_failed_response(err, operation.to_string())),
        }
    }

    #[fastrace::trace]
    pub(crate) async fn submit_statement(
        &self,
        request: StatementRequest,
    ) -> Result<Response<StatementStatus>, Error> {
        let url = self.make_url("v1/statements")?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to submit statement: {request:?}"),
                )
                .set_source(err)
            })?;
        Response::from_http_response(response).await
    }

    #[fastrace::trace]
    pub(crate) async fn fetch_statement(
        &self,
        statement_id: Uuid,
        params: StatementRequestParams,
    ) -> Result<Response<StatementStatus>, Error> {
        let path = format!("v1/statements/{statement_id}");
        let url = self.make_url(&path)?;
        let response = self
            .client
            .get(url)
            .headers(traceparent_headers())
            .query(&params)
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to fetch statement: {statement_id:?}"),
                )
                .set_source(err)
            })?;
        Response::from_http_response(response).await
    }

    #[fastrace::trace]
    pub(crate) async fn cancel_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<Response<StatementCancelResult>, Error> {
        let path = format!("v1/statements/{statement_id}/cancel");
        let url = self.make_url(&path)?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to cancel statement: {statement_id:?}"),
                )
                .set_source(err)
            })?;
        Response::from_http_response(response).await
    }

    #[fastrace::trace]
    pub(crate) async fn ingest(
        &self,
        request: IngestRequest,
    ) -> Result<Response<IngestResult>, Error> {
        let format = request.data.format();
        let url = self.make_url("v1/ingest")?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to ingest data in {format}"),
                )
                .set_source(err)
            })?;
        Response::from_http_response(response).await
    }

    #[track_caller]
    fn make_url(&self, path: &str) -> Result<Url, Error> {
        self.endpoint.join(path).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to construct URL".to_string()).set_source(err)
        })
    }

    fn make_resource_url(&self, segments: &[&str]) -> Result<Url, Error> {
        let mut url = self.make_url("v1/")?;
        let mut path = url.path_segments_mut().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "endpoint cannot be used as a base URL",
            )
        })?;
        path.pop_if_empty();
        path.extend(segments.iter().copied());
        drop(path);
        Ok(url)
    }
}

fn map_failed_response(err: crate::protocol::ErrorStatus, message: String) -> Error {
    let error = Error::new(ErrorKind::Unexpected, format!("{message}: {err}"));
    match err.code() {
        reqwest::StatusCode::REQUEST_TIMEOUT
        | reqwest::StatusCode::TOO_MANY_REQUESTS
        | reqwest::StatusCode::BAD_GATEWAY
        | reqwest::StatusCode::SERVICE_UNAVAILABLE
        | reqwest::StatusCode::GATEWAY_TIMEOUT => error.set_temporary(),
        code if code.is_server_error() => error.set_temporary(),
        _ => error.set_permanent(),
    }
}

fn decode_append_response(status: StatusCode, payload: &[u8]) -> Result<AppendRowsResult, Error> {
    if status == StatusCode::OK {
        return match serde_json::from_slice::<AppendRowsResult>(payload) {
            Ok(result)
                if result.append_state == AppendState::Committed
                    && result.num_rows_inserted >= 0 =>
            {
                Ok(result)
            }
            Ok(_) => Err(append_unknown_error(
                "table append response has an invalid body",
            )),
            Err(err) => {
                Err(append_unknown_error("failed to decode table append response").set_source(err))
            }
        };
    }

    if status.is_success() {
        return Err(append_unknown_error(format!(
            "table append returned unexpected success status {}",
            status.as_u16()
        )));
    }

    if let Ok(payload) = serde_json::from_slice::<AppendRowsErrorPayload>(payload) {
        if matches!(
            payload.details.append_state,
            AppendState::Rejected | AppendState::Unknown
        ) {
            let append_state = payload.details.append_state;
            let error = Error::new(
                ErrorKind::AppendRowsFailed,
                format_http_error(status, &payload.message),
            )
            .set_append_details(payload.details);

            return Err(if append_state == AppendState::Unknown {
                error.set_persistent()
            } else if is_temporary_http_status(status) {
                error.set_temporary()
            } else {
                error.set_permanent()
            });
        }
    }

    #[derive(Deserialize)]
    struct ErrorMessage {
        message: String,
    }

    let message = serde_json::from_slice::<ErrorMessage>(payload)
        .map(|payload| payload.message)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned());
    Err(append_unknown_error(format_http_error(status, &message)))
}

fn append_unknown_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::AppendRowsFailed, message)
        .set_append_details(AppendErrorDetails {
            append_state: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        })
        .set_persistent()
}

fn is_temporary_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

fn format_http_error(status: StatusCode, message: &str) -> String {
    format!(
        "{} ({}): {}",
        status.canonical_reason().unwrap_or("Unknown"),
        status.as_u16(),
        message
    )
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::Client;
    use super::decode_append_response;
    use crate::ErrorKind;
    use crate::protocol::AppendState;

    #[test]
    fn resource_url_preserves_base_path_and_encodes_segments() {
        let client = Client::new("https://example.com/proxy", reqwest::Client::new()).unwrap();

        let url = client
            .make_resource_url(&["databases", "analytics/2026", "schemas", "events archive"])
            .unwrap();

        assert_eq!(
            url.as_str(),
            "https://example.com/proxy/v1/databases/analytics%2F2026/schemas/events%20archive"
        );
    }

    #[test]
    fn committed_append_response_is_returned() {
        let result = decode_append_response(
            StatusCode::OK,
            br#"{"append_state":"committed","num_rows_inserted":2}"#,
        )
        .unwrap();

        assert_eq!(result.append_state, AppendState::Committed);
        assert_eq!(result.num_rows_inserted, 2);
    }

    #[test]
    fn rejected_append_error_preserves_row_details_and_retry_status() {
        let error = decode_append_response(
            StatusCode::SERVICE_UNAVAILABLE,
            br#"{
                "message":"validation failed",
                "append_state":"rejected",
                "row_errors":[{"row_index":3,"column":"ts","message":"invalid timestamp"}],
                "row_errors_truncated":true
            }"#,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::AppendRowsFailed);
        assert!(error.is_temporary());
        let details = error.append_details().unwrap();
        assert_eq!(details.append_state, AppendState::Rejected);
        assert_eq!(details.row_errors[0].row_index, 3);
        assert!(details.row_errors_truncated);
    }

    #[test]
    fn unknown_append_outcome_is_never_retryable() {
        let error = decode_append_response(
            StatusCode::SERVICE_UNAVAILABLE,
            br#"{"message":"coordinator unavailable","append_state":"unknown"}"#,
        )
        .unwrap_err();

        assert!(error.is_persistent());
        assert!(!error.is_temporary());
        assert_eq!(
            error.append_details().unwrap().append_state,
            AppendState::Unknown
        );
    }

    #[test]
    fn unstructured_append_error_is_treated_as_unknown() {
        let error =
            decode_append_response(StatusCode::BAD_REQUEST, br#"{"message":"bad request"}"#)
                .unwrap_err();

        assert!(error.is_persistent());
        assert_eq!(
            error.append_details().unwrap().append_state,
            AppendState::Unknown
        );
    }
}
