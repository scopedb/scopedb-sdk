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

use std::time::Duration;

use exn::IntoExn;
use exn::Result;
use jiff::SignedDuration;
use uuid::Uuid;

use crate::Error;
use crate::StatementCancelResponse;
use crate::client::Client;
use crate::protocol::Response;
use crate::protocol::ResultFormat;
use crate::protocol::StatementEstimatedProgress;
use crate::protocol::StatementRequest;
use crate::protocol::StatementRequestParams;
use crate::protocol::StatementResponse;
use crate::result::ResultSet;

#[derive(Debug)]
pub struct Statement {
    client: Client,
    statement: String,
    statement_id: Option<Uuid>,
    exec_timeout: Option<SignedDuration>,
    format: ResultFormat,
}

impl Statement {
    pub fn with_statement_id(mut self, statement_id: Uuid) -> Self {
        self.statement_id = Some(statement_id);
        self
    }

    pub fn with_exec_timeout(mut self, exec_timeout: SignedDuration) -> Self {
        self.exec_timeout = Some(exec_timeout);
        self
    }

    pub async fn submit(self) -> Result<StatementHandle, Error> {
        let Statement {
            client,
            statement,
            statement_id,
            exec_timeout,
            format,
        } = self;

        let resp = client
            .submit_statement(StatementRequest {
                statement,
                statement_id,
                exec_timeout,
                params: StatementRequestParams { format },
            })
            .await?;

        match resp {
            Response::Success(response) => Ok(StatementHandle {
                client,
                statement_id: response.statement_id(),
                format,
                response: Some(response),
            }),
            Response::Failed(err) => {
                Err(Error(format!("failed to submit statement: {err}")).into_exn())
            }
        }
    }

    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            statement_id: None,
            exec_timeout: None,
            format: ResultFormat::Json,
        }
    }
}

#[derive(Debug)]
pub struct StatementHandle {
    client: Client,
    statement_id: Uuid,
    format: ResultFormat,
    response: Option<StatementResponse>,
}

impl StatementHandle {
    pub fn statement_id(&self) -> Uuid {
        self.statement_id
    }

    pub fn status(&self) -> Option<&str> {
        self.response.as_ref().map(|r| r.status())
    }

    pub fn is_terminated(&self) -> bool {
        self.response.as_ref().is_some_and(|r| r.is_terminated())
    }

    pub fn progress(&self) -> Option<&StatementEstimatedProgress> {
        self.response.as_ref().map(|r| r.progress())
    }

    pub fn result_set(&self) -> Option<ResultSet> {
        let result_set = self.response.as_ref().and_then(|r| r.result_set())?;
        Some(ResultSet::from_statement_result_set(result_set.clone()))
    }

    pub async fn fetch_once(&mut self) -> Result<(), Error> {
        if self.is_terminated() {
            return Ok(());
        }

        let resp = self
            .client
            .fetch_statement(
                self.statement_id,
                StatementRequestParams {
                    format: self.format,
                },
            )
            .await?;

        match resp {
            Response::Success(response) => {
                self.response = Some(response);
                Ok(())
            }
            Response::Failed(err) => {
                Err(Error(format!("failed to submit statement: {err}")).into_exn())
            }
        }
    }

    pub async fn fetch(mut self) -> Result<ResultSet, Error> {
        while !self.is_terminated() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.fetch_once().await?;
        }

        match self.response.unwrap() {
            StatementResponse::Finished { result_set, .. } => {
                Ok(ResultSet::from_statement_result_set(result_set.clone()))
            }
            StatementResponse::Failed { message, .. } => {
                Err(Error(format!("statement failed: {message}")).into_exn())
            }
            StatementResponse::Cancelled { message, .. } => {
                Err(Error(format!("statement cancelled: {message}")).into_exn())
            }
            StatementResponse::Pending { .. } => {
                unreachable!("pending statements should not be fetched")
            }
            StatementResponse::Running { .. } => {
                unreachable!("running statements should not be fetched")
            }
        }
    }

    pub async fn cancel(&mut self) -> Result<StatementCancelResponse, Error> {
        if let Some(response) = self.response.as_ref() {
            match response {
                StatementResponse::Pending { .. } | StatementResponse::Running { .. } => {}
                StatementResponse::Finished {
                    statement_id,
                    created_at,
                    ..
                } => {
                    return Ok(StatementCancelResponse {
                        statement_id: *statement_id,
                        created_at: *created_at,
                        status: "finished".to_string(),
                        message: "statement is finished".to_string(),
                    });
                }
                StatementResponse::Failed {
                    statement_id,
                    created_at,
                    ..
                } => {
                    return Ok(StatementCancelResponse {
                        statement_id: *statement_id,
                        created_at: *created_at,
                        status: "failed".to_string(),
                        message: "statement is failed".to_string(),
                    });
                }
                StatementResponse::Cancelled {
                    statement_id,
                    created_at,
                    ..
                } => {
                    return Ok(StatementCancelResponse {
                        statement_id: *statement_id,
                        created_at: *created_at,
                        status: "cancelled".to_string(),
                        message: "statement is cancelled".to_string(),
                    });
                }
            }
        }

        match self.client.cancel_statement(self.statement_id).await? {
            Response::Success(response) => Ok(response),
            Response::Failed(err) => {
                Err(Error(format!("failed to cancel statement: {err}")).into_exn())
            }
        }
    }

    pub(crate) fn new(client: Client, statement_id: Uuid, format: ResultFormat) -> Self {
        Self {
            client,
            statement_id,
            format,
            response: None,
        }
    }
}
