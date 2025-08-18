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

use exn::Result;
use exn::ResultExt;
use fastrace_reqwest::traceparent_headers;
use reqwest::IntoUrl;
use reqwest::Url;
use uuid::Uuid;

use crate::Error;
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
        let endpoint = endpoint
            .into_url()
            .or_raise(|| Error("failed to parse endpoint".to_string()))?;

        Ok(Self { endpoint, client })
    }

    pub fn statement(&self, statement: String) -> Statement {
        Statement::new(self.clone(), statement)
    }

    pub fn statement_handle(&self, statement_id: Uuid) -> StatementHandle {
        StatementHandle::new(self.clone(), statement_id, ResultFormat::Json)
    }

    pub async fn health_check(&self) -> Result<(), Error> {
        let make_error = || Error("failed to do health check".to_string());
        let url = self.endpoint.join("v1/health").or_raise(make_error)?;
        self.client.get(url).send().await.or_raise(make_error)?;
        Ok(())
    }

    #[fastrace::trace]
    pub async fn ingest(&self, request: IngestRequest) -> Result<Response<IngestResult>, Error> {
        let format = request.data.format();
        let make_error = || Error(format!("failed to ingest data in {format}"));
        let url = self.endpoint.join("v1/ingest").or_raise(make_error)?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .json(&request)
            .send()
            .await
            .or_raise(make_error)?;
        Response::from_http_response(response).await
    }
}

impl Client {
    #[fastrace::trace]
    pub(crate) async fn submit_statement(
        &self,
        request: StatementRequest,
    ) -> Result<Response<StatementStatus>, Error> {
        let make_error = || Error(format!("failed to submit statement: {request:?}"));
        let url = self.endpoint.join("v1/statements").or_raise(make_error)?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .json(&request)
            .send()
            .await
            .or_raise(make_error)?;
        Response::from_http_response(response).await
    }

    #[fastrace::trace]
    pub(crate) async fn fetch_statement(
        &self,
        statement_id: Uuid,
        params: StatementRequestParams,
    ) -> Result<Response<StatementStatus>, Error> {
        let make_error = || Error(format!("failed to fetch statement: {statement_id:?}"));
        let path = format!("v1/statements/{statement_id}");
        let url = self.endpoint.join(&path).or_raise(make_error)?;
        let response = self
            .client
            .get(url)
            .headers(traceparent_headers())
            .query(&params)
            .send()
            .await
            .or_raise(make_error)?;
        Response::from_http_response(response).await
    }

    #[fastrace::trace]
    pub(crate) async fn cancel_statement(
        &self,
        statement_id: Uuid,
    ) -> Result<Response<StatementCancelResult>, Error> {
        let make_error = || Error(format!("failed to cancel statement: {statement_id:?}"));
        let path = format!("v1/statements/{statement_id}/cancel");
        let url = self.endpoint.join(&path).or_raise(make_error)?;
        let response = self
            .client
            .post(url)
            .headers(traceparent_headers())
            .send()
            .await
            .or_raise(make_error)?;
        Response::from_http_response(response).await
    }
}
