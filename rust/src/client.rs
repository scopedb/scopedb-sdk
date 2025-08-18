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
use reqwest::Url;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::Statement;
use crate::protocol::IngestRequest;
use crate::protocol::IngestResult;
use crate::protocol::Response;
use crate::protocol::ResultFormat;
use crate::protocol::StatementCancelResult;
use crate::protocol::StatementRequest;
use crate::protocol::StatementRequestParams;
use crate::protocol::StatementStatus;
use crate::statement::StatementHandle;

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn new<E: IntoUrl>(endpoint: E, client: reqwest::Client) -> Result<Self, Error> {
        match endpoint.into_url() {
            Ok(endpoint) => Ok(Self { endpoint, client }),
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

    #[fastrace::trace]
    pub async fn ingest(&self, request: IngestRequest) -> Result<Response<IngestResult>, Error> {
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
}

impl Client {
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

    #[track_caller]
    fn make_url(&self, path: &str) -> Result<Url, Error> {
        self.endpoint.join(path).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to construct URL".to_string()).set_source(err)
        })
    }
}
