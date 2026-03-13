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

use jiff::SignedDuration;
use tokio::time::sleep;
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
    max_parallelism: Option<usize>,
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

    pub fn with_max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    pub async fn submit(self) -> Result<StatementHandle, Error> {
        let Statement {
            client,
            statement,
            statement_id,
            exec_timeout,
            max_parallelism,
            format,
        } = self;

        let resp = client
            .submit_statement(StatementRequest {
                statement,
                statement_id,
                exec_timeout,
                max_parallelism,
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

    pub async fn execute(self) -> Result<ResultSet, Error> {
        self.submit().await?.fetch().await
    }

    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            statement_id: None,
            exec_timeout: None,
            max_parallelism: None,
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

    pub fn progress(&self) -> Option<&crate::StatementEstimatedProgress> {
        self.status.as_ref().map(StatementStatus::progress)
    }

    pub fn result_set(&self) -> Option<ResultSet> {
        self.status.as_ref().and_then(|status| match status {
            StatementStatus::Finished(s) => Some(s.result_set()),
            _ => None,
        })
    }

    pub async fn fetch_once(&mut self) -> Result<(), Error> {
        // already terminated - no need to fetch again
        match self.status.as_ref() {
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

    pub async fn fetch(&mut self) -> Result<ResultSet, Error> {
        let mut delay = Duration::from_millis(5);
        let max_delay = Duration::from_secs(1);

        loop {
            self.fetch_once().await?;

            if let Some(status) = self.status.as_ref() {
                match status {
                    StatementStatus::Finished(finished) => return Ok(finished.result_set()),
                    StatementStatus::Failed(failed) => {
                        return Err(Error::new(ErrorKind::Unexpected, failed.message.clone()));
                    }
                    StatementStatus::Cancelled(cancelled) => {
                        return Err(Error::new(ErrorKind::Unexpected, cancelled.message.clone()));
                    }
                    StatementStatus::Pending(..) | StatementStatus::Running(..) => {
                        sleep(delay).await;
                        if delay < max_delay {
                            delay = std::cmp::min(delay.saturating_mul(2), max_delay);
                        }
                    }
                }
            }
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
            Response::Success(response) => {
                self.status = match response.status.as_str() {
                    "failed" => Some(StatementStatus::Failed(crate::StatementStatusFailed {
                        statement_id: response.statement_id,
                        created_at: response.created_at,
                        progress: crate::StatementEstimatedProgress::default(),
                        message: response.message.clone(),
                    })),
                    "cancelled" => Some(StatementStatus::Cancelled(
                        crate::StatementStatusCancelled {
                            statement_id: response.statement_id,
                            created_at: response.created_at,
                            progress: crate::StatementEstimatedProgress::default(),
                            message: response.message.clone(),
                        },
                    )),
                    _ => self.status.take(),
                };
                Ok(response)
            }
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
