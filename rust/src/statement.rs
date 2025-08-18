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

use jiff::SignedDuration;
use uuid::Uuid;

use crate::Error;
use crate::ErrorKind;
use crate::StatementCancelResult;
use crate::client::Client;
use crate::protocol::Response;
use crate::protocol::ResultFormat;
use crate::protocol::StatementRequest;
use crate::protocol::StatementRequestParams;
use crate::protocol::StatementStatus;
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
                status: Some(response),
            }),
            Response::Failed(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("failed to submit statement: {err}"),
            )),
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
    status: Option<StatementStatus>,
}

impl StatementHandle {
    pub fn statement_id(&self) -> Uuid {
        self.statement_id
    }

    pub fn status(&self) -> Option<&StatementStatus> {
        self.status.as_ref()
    }

    pub fn result_set(&self) -> Option<ResultSet> {
        self.status.as_ref().and_then(|status| match status {
            StatementStatus::Finished(s) => Some(s.result_set()),
            _ => None,
        })
    }

    pub async fn fetch_once(&mut self) -> Result<(), Error> {
        // already terminated - no need to fetch again
        match self.status {
            Some(StatementStatus::Finished(..))
            | Some(StatementStatus::Failed(..))
            | Some(StatementStatus::Cancelled(..)) => {
                return Ok(());
            }
            _ => {}
        }

        let (format, statement_id) = (self.format, self.statement_id);
        match self
            .client
            .fetch_statement(statement_id, StatementRequestParams { format })
            .await?
        {
            Response::Success(status) => {
                self.status = Some(status);
                Ok(())
            }
            Response::Failed(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("failed to fetch statement: {err}"),
            )),
        }
    }

    pub async fn cancel(&mut self) -> Result<StatementCancelResult, Error> {
        if let Some(response) = self.status.as_ref() {
            match response {
                StatementStatus::Pending(..) | StatementStatus::Running(..) => {}
                StatementStatus::Finished(s) => {
                    return Ok(StatementCancelResult {
                        statement_id: s.statement_id,
                        created_at: s.created_at,
                        status: "finished".to_string(),
                        message: "statement is finished".to_string(),
                    });
                }
                StatementStatus::Failed(s) => {
                    return Ok(StatementCancelResult {
                        statement_id: s.statement_id,
                        created_at: s.created_at,
                        status: "failed".to_string(),
                        message: "statement is failed".to_string(),
                    });
                }
                StatementStatus::Cancelled(s) => {
                    return Ok(StatementCancelResult {
                        statement_id: s.statement_id,
                        created_at: s.created_at,
                        status: "cancelled".to_string(),
                        message: "statement is cancelled".to_string(),
                    });
                }
            }
        }

        match self.client.cancel_statement(self.statement_id).await? {
            Response::Success(response) => Ok(response),
            Response::Failed(err) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("failed to cancel statement: {err}"),
            )),
        }
    }

    pub(crate) fn new(client: Client, statement_id: Uuid, format: ResultFormat) -> Self {
        Self {
            client,
            statement_id,
            format,
            status: None,
        }
    }
}
