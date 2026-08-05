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
use reqwest::Method;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::CatalogIterator;
use crate::Error;
use crate::ErrorKind;
use crate::IngestStreamBuilder;
use crate::ResultSet;
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
use crate::protocol::apply_response_metadata;
use crate::statement::StatementHandle;

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Url,
    client: reqwest::Client,
    authorization: Option<HeaderValue>,
}

/// Builds a ScopeDB client with an optional API key and custom HTTP client.
///
/// The API key is attached by the ScopeDB client itself, so callers can still
/// provide a preconfigured reqwest client for TLS, proxy, timeout, and pooling
/// settings.
pub struct ClientBuilder {
    endpoint: String,
    client: Option<reqwest::Client>,
    api_key: Option<String>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("endpoint", &self.endpoint)
            .field("client", &self.client)
            .field("has_api_key", &self.api_key.is_some())
            .finish()
    }
}

impl ClientBuilder {
    /// Sets the ScopeDB API key sent as a sensitive Bearer credential.
    ///
    /// This per-request value takes precedence over an `Authorization` default
    /// configured on the underlying HTTP client.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Uses a preconfigured compatible HTTP client.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Validates the endpoint and authentication settings and builds the client.
    pub fn build(self) -> Result<Client, Error> {
        let mut client = Client::new(self.endpoint, self.client.unwrap_or_default())?;
        if let Some(api_key) = self.api_key {
            if api_key.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "api_key must not be empty",
                ));
            }
            let mut authorization =
                HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|err| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "api_key is not a valid HTTP credential",
                    )
                    .set_source(err)
                })?;
            authorization.set_sensitive(true);
            client.authorization = Some(authorization);
        }
        Ok(client)
    }
}

impl Client {
    /// Starts a client builder for the common API-key authentication path.
    pub fn builder(endpoint: impl ToString) -> ClientBuilder {
        ClientBuilder {
            endpoint: endpoint.to_string(),
            client: None,
            api_key: None,
        }
    }

    /// Creates a ScopeDB client from an endpoint and a compatible HTTP client.
    ///
    /// Use the HTTP types re-exported from [`crate::reqwest`] to avoid a
    /// dependency-version mismatch in applications using another reqwest major.
    pub fn new<E: IntoUrl>(endpoint: E, client: reqwest::Client) -> Result<Self, Error> {
        match endpoint.into_url() {
            Ok(mut endpoint) => {
                if !endpoint.path().ends_with('/') {
                    let path = format!("{}/", endpoint.path());
                    endpoint.set_path(&path);
                }
                Ok(Self {
                    endpoint,
                    client,
                    authorization: None,
                })
            }
            Err(err) => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "failed to parse endpoint".to_string(),
            )
            .set_source(err)),
        }
    }

    pub fn statement(&self, statement: impl Into<String>) -> Statement {
        Statement::new(self.clone(), statement.into())
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

    /// Executes a ScopeQL statement and waits for all result rows.
    pub async fn query(&self, statement: impl Into<String>) -> Result<ResultSet, Error> {
        self.statement(statement).execute().await
    }

    pub async fn list_databases(
        &self,
        options: CatalogListOptions,
    ) -> Result<CatalogPage<DatabaseResource>, Error> {
        self.fetch_catalog(&["databases"], Some(&options), "failed to list databases")
            .await
    }

    /// Lazily iterates databases across every catalog page.
    pub fn iterate_databases(
        &self,
        options: CatalogListOptions,
    ) -> CatalogIterator<DatabaseResource> {
        let client = self.clone();
        CatalogIterator::new(options, move |options| {
            let client = client.clone();
            async move { client.list_databases(options).await }
        })
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

    /// Lazily iterates schemas across every catalog page.
    pub fn iterate_schemas(
        &self,
        database: impl Into<String>,
        options: CatalogListOptions,
    ) -> CatalogIterator<SchemaResource> {
        let client = self.clone();
        let database = database.into();
        CatalogIterator::new(options, move |options| {
            let client = client.clone();
            let database = database.clone();
            async move { client.list_schemas(&database, options).await }
        })
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

    /// Lazily iterates table summaries across every catalog page.
    pub fn iterate_tables(
        &self,
        database: impl Into<String>,
        schema: impl Into<String>,
        options: CatalogListOptions,
    ) -> CatalogIterator<TableResourceSummary> {
        let client = self.clone();
        let database = database.into();
        let schema = schema.into();
        CatalogIterator::new(options, move |options| {
            let client = client.clone();
            let database = database.clone();
            let schema = schema.clone();
            async move { client.list_tables(&database, &schema, options).await }
        })
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
            .request(Method::POST, url)
            .headers(traceparent_headers())
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .map_err(|err| {
                append_unknown_error("failed to send table append request").set_source(err)
            })?;

        let status = response.status();
        let headers = response.headers().clone();
        let payload = response.bytes().await.map_err(|err| {
            apply_response_metadata(
                append_unknown_error("failed to read table append response").set_source(err),
                status,
                &headers,
            )
        })?;
        let result = decode_append_response(status, &headers, &payload)?;
        if result.num_rows_inserted != expected_rows as i64 {
            return Err(apply_response_metadata(
                append_unknown_error(format!(
                    "table append response reported {} inserted rows for a {expected_rows}-row request",
                    result.num_rows_inserted
                )),
                status,
                &headers,
            ));
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
            Response::Failed(err) => Err(map_failed_response(err)),
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
        if let Some(options) = options {
            options.validate()?;
        }
        let url = self.make_resource_url(segments)?;
        let mut request = self
            .request(Method::GET, url)
            .headers(traceparent_headers());
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
            Response::Failed(err) => Err(map_failed_response(err)),
        }
    }

    #[fastrace::trace]
    pub(crate) async fn submit_statement(
        &self,
        request: StatementRequest,
    ) -> Result<Response<StatementStatus>, Error> {
        let url = self.make_url("v1/statements")?;
        let response = self
            .request(Method::POST, url)
            .headers(traceparent_headers())
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to submit statement").set_source(err)
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
            .request(Method::GET, url)
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
                .set_temporary()
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
            .request(Method::POST, url)
            .headers(traceparent_headers())
            .send()
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to cancel statement: {statement_id:?}"),
                )
                .set_source(err)
                .set_temporary()
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
            .request(Method::POST, url)
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

    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let request = self.client.request(method, url);
        if let Some(authorization) = &self.authorization {
            request.header(AUTHORIZATION, authorization.clone())
        } else {
            request
        }
    }
}

fn map_failed_response(err: crate::protocol::ErrorStatus) -> Error {
    err.into_error(ErrorKind::Unexpected)
}

fn decode_append_response(
    status: StatusCode,
    headers: &reqwest::header::HeaderMap,
    payload: &[u8],
) -> Result<AppendRowsResult, Error> {
    if status == StatusCode::OK {
        return match serde_json::from_slice::<AppendRowsResult>(payload) {
            Ok(result)
                if result.append_state == AppendState::Committed
                    && result.num_rows_inserted >= 0 =>
            {
                Ok(result)
            }
            Ok(_) => Err(apply_response_metadata(
                append_unknown_error("table append response has an invalid body"),
                status,
                headers,
            )),
            Err(err) => Err(apply_response_metadata(
                append_unknown_error("failed to decode table append response").set_source(err),
                status,
                headers,
            )),
        };
    }

    if status.is_success() {
        return Err(apply_response_metadata(
            append_unknown_error(format!(
                "table append returned unexpected success status {}",
                status.as_u16()
            )),
            status,
            headers,
        ));
    }

    let http_error = crate::protocol::ErrorStatus::from_http_parts(status, headers, payload);
    if let Ok(payload) = serde_json::from_slice::<AppendRowsErrorPayload>(payload) {
        if matches!(
            payload.details.append_state,
            AppendState::Rejected | AppendState::Unknown
        ) {
            let append_state = payload.details.append_state;
            let error = http_error
                .into_error(ErrorKind::AppendRowsFailed)
                .set_append_details(payload.details);

            return Err(if append_state == AppendState::Unknown {
                error.set_persistent()
            } else {
                error
            });
        }
    }

    let metadata_error = http_error.into_error(ErrorKind::AppendRowsFailed);
    let mut error =
        append_unknown_error(metadata_error.message().to_string()).set_http_status(status);
    if let Some(request_id) = metadata_error.request_id() {
        error = error.set_request_id(request_id.to_string());
    }
    if let Some(retry_after) = metadata_error.retry_after() {
        error = error.set_retry_after(retry_after);
    }
    Err(error)
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

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use reqwest::header::HeaderMap;

    use super::Client;
    use super::decode_append_response;
    use crate::ErrorKind;
    use crate::protocol::AppendState;

    fn decode(status: StatusCode, payload: &[u8]) -> Result<crate::AppendRowsResult, crate::Error> {
        decode_append_response(status, &HeaderMap::new(), payload)
    }

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
    fn client_builder_validates_and_redacts_api_keys() {
        let builder = Client::builder("https://example.com").api_key("secret-api-key");
        assert!(!format!("{builder:?}").contains("secret-api-key"));
        let client = Client::builder("https://example.com")
            .api_key("secret-api-key")
            .build()
            .unwrap();
        assert!(!format!("{client:?}").contains("secret-api-key"));

        let error = Client::builder("https://example.com")
            .api_key("")
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn committed_append_response_is_returned() {
        let result = decode(
            StatusCode::OK,
            br#"{"append_state":"committed","num_rows_inserted":2}"#,
        )
        .unwrap();

        assert_eq!(result.append_state, AppendState::Committed);
        assert_eq!(result.num_rows_inserted, 2);
    }

    #[test]
    fn rejected_append_error_preserves_row_details_and_retry_status() {
        let error = decode(
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
    fn append_error_preserves_server_message_and_http_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "header-request".parse().unwrap());
        headers.insert(reqwest::header::RETRY_AFTER, "3".parse().unwrap());
        let error = decode_append_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &headers,
            br#"{
                "message":"capacity is temporarily unavailable",
                "request_id":"body-request",
                "retryable":false,
                "append_state":"rejected"
            }"#,
        )
        .unwrap_err();

        assert_eq!(error.message(), "capacity is temporarily unavailable");
        assert_eq!(error.http_status(), Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(error.request_id(), Some("body-request"));
        assert_eq!(error.retry_after(), Some(std::time::Duration::from_secs(3)));
        assert!(!error.is_retryable());
    }

    #[test]
    fn unknown_append_outcome_is_never_retryable() {
        let error = decode(
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
        let error = decode(StatusCode::BAD_REQUEST, br#"{"message":"bad request"}"#).unwrap_err();

        assert!(error.is_persistent());
        assert_eq!(
            error.append_details().unwrap().append_state,
            AppendState::Unknown
        );
    }

    #[test]
    fn nested_append_error_has_clean_message_and_unknown_outcome() {
        let error = decode(
            StatusCode::NOT_FOUND,
            br#"{"error":{"message":"unsupported path"}}"#,
        )
        .unwrap_err();

        assert_eq!(error.message(), "unsupported path");
        assert!(error.is_persistent());
        assert_eq!(
            error.append_details().unwrap().append_state,
            AppendState::Unknown
        );
    }
}
