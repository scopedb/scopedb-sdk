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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::Client;
use crate::Error;
use crate::ErrorKind;
use crate::IngestData;
use crate::IngestResult;

const DEFAULT_BATCH_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
struct FatalState {
    message: String,
    temporary: bool,
}

impl FatalState {
    fn from_error(error: &Error) -> Self {
        Self {
            message: error.to_string(),
            temporary: error.is_temporary(),
        }
    }

    fn into_error(self) -> Error {
        let error = Error::new(ErrorKind::Unexpected, self.message);
        if self.temporary {
            error.set_temporary()
        } else {
            error.set_persistent()
        }
    }
}

enum BatchCommand {
    Record(String),
    Flush(oneshot::Sender<Result<Option<IngestResult>, Error>>),
    Shutdown(oneshot::Sender<Result<Option<IngestResult>, Error>>),
}

pub struct JsonBatcherBuilder {
    client: Client,
    statement: String,
    batch_bytes: usize,
    flush_interval: Duration,
    channel_capacity: usize,
}

impl JsonBatcherBuilder {
    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            batch_bytes: DEFAULT_BATCH_BYTES,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    pub fn batch_bytes(mut self, batch_bytes: usize) -> Self {
        self.batch_bytes = batch_bytes;
        self
    }

    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    pub fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    pub fn build(self) -> JsonBatcher {
        JsonBatcher::new(
            self.client,
            self.statement,
            self.batch_bytes,
            self.flush_interval,
            self.channel_capacity,
        )
    }
}

pub struct JsonBatcher {
    tx: mpsc::Sender<BatchCommand>,
    task: Mutex<Option<JoinHandle<()>>>,
    fatal: Arc<Mutex<Option<FatalState>>>,
}

impl JsonBatcher {
    fn new(
        client: Client,
        statement: String,
        batch_bytes: usize,
        flush_interval: Duration,
        channel_capacity: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        let fatal = Arc::new(Mutex::new(None));
        let task = tokio::spawn(run_batch_worker(
            rx,
            batch_bytes,
            flush_interval,
            fatal.clone(),
            move |rows| {
                let client = client.clone();
                let statement = statement.clone();
                Box::pin(async move { client.insert(IngestData::Json { rows }, statement).await })
            },
        ));

        Self {
            tx,
            task: Mutex::new(Some(task)),
            fatal,
        }
    }

    pub async fn send<T: Serialize>(&self, record: &T) -> Result<(), Error> {
        self.check_fatal()?;
        let payload = serde_json::to_string(record).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to serialize batched ingest record".to_string(),
            )
            .set_source(err)
        })?;

        self.tx
            .send(BatchCommand::Record(payload))
            .await
            .map_err(|_| self.closed_or_fatal_error())?;
        self.check_fatal()
    }

    pub async fn flush(&self) -> Result<Option<IngestResult>, Error> {
        self.check_fatal()?;
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(BatchCommand::Flush(tx))
            .await
            .map_err(|_| self.closed_or_fatal_error())?;
        rx.await.map_err(|_| self.closed_or_fatal_error())?
    }

    pub async fn shutdown(self) -> Result<Option<IngestResult>, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(BatchCommand::Shutdown(tx))
            .await
            .map_err(|_| self.closed_or_fatal_error())?;

        let result = rx.await.map_err(|_| self.closed_or_fatal_error())?;
        let task = self.task.lock().expect("lock poisoned").take();
        if let Some(task) = task {
            task.await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "json batcher background task panicked".to_string(),
                )
                .set_source(err)
            })?;
        }
        result
    }

    fn check_fatal(&self) -> Result<(), Error> {
        if let Some(state) = self.fatal.lock().expect("lock poisoned").clone() {
            Err(state.into_error())
        } else {
            Ok(())
        }
    }

    fn closed_or_fatal_error(&self) -> Error {
        self.fatal
            .lock()
            .expect("lock poisoned")
            .clone()
            .map(FatalState::into_error)
            .unwrap_or_else(|| {
                Error::new(ErrorKind::Unexpected, "json batcher is closed".to_string())
                    .set_persistent()
            })
    }
}

type BoxFutureIngest = Pin<Box<dyn Future<Output = Result<IngestResult, Error>> + Send>>;

async fn run_batch_worker<F>(
    mut rx: mpsc::Receiver<BatchCommand>,
    batch_bytes: usize,
    flush_interval: Duration,
    fatal: Arc<Mutex<Option<FatalState>>>,
    mut flush_fn: F,
) where
    F: FnMut(String) -> BoxFutureIngest + Send + 'static,
{
    let mut rows = Vec::new();
    let mut current_bytes = 0usize;
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if rows.is_empty() {
                    continue;
                }
                if let Err(err) = flush_pending(&mut rows, &mut current_bytes, &mut flush_fn).await {
                    *fatal.lock().expect("lock poisoned") = Some(FatalState::from_error(&err));
                    break;
                }
            }
            command = rx.recv() => {
                let Some(command) = command else {
                    if let Err(err) = flush_pending(&mut rows, &mut current_bytes, &mut flush_fn).await {
                        *fatal.lock().expect("lock poisoned") = Some(FatalState::from_error(&err));
                    }
                    break;
                };

                match command {
                    BatchCommand::Record(payload) => {
                        current_bytes = current_bytes.saturating_add(payload.len());
                        rows.push(payload);
                        if !rows.is_empty() && current_bytes >= batch_bytes {
                            if let Err(err) = flush_pending(&mut rows, &mut current_bytes, &mut flush_fn).await {
                                *fatal.lock().expect("lock poisoned") = Some(FatalState::from_error(&err));
                                break;
                            }
                        }
                    }
                    BatchCommand::Flush(ack) => {
                        let result = flush_pending(&mut rows, &mut current_bytes, &mut flush_fn).await;
                        if let Err(err) = &result {
                            *fatal.lock().expect("lock poisoned") = Some(FatalState::from_error(err));
                        }
                        let _ = ack.send(result);
                        if fatal.lock().expect("lock poisoned").is_some() {
                            break;
                        }
                    }
                    BatchCommand::Shutdown(ack) => {
                        let result = flush_pending(&mut rows, &mut current_bytes, &mut flush_fn).await;
                        if let Err(err) = &result {
                            *fatal.lock().expect("lock poisoned") = Some(FatalState::from_error(err));
                        }
                        let _ = ack.send(result);
                        break;
                    }
                }
            }
        }
    }
}

async fn flush_pending<F>(
    rows: &mut Vec<String>,
    current_bytes: &mut usize,
    flush_fn: &mut F,
) -> Result<Option<IngestResult>, Error>
where
    F: FnMut(String) -> BoxFutureIngest,
{
    if rows.is_empty() {
        return Ok(None);
    }

    let payload = rows.join("\n");
    rows.clear();
    *current_bytes = 0;
    flush_fn(payload).await.map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_error(message: &str) -> Error {
        Error::new(ErrorKind::Unexpected, message.to_string())
    }

    #[tokio::test]
    async fn test_flush_pending_joins_rows() {
        let mut rows = vec!["{\"a\":1}".to_string(), "{\"a\":2}".to_string()];
        let mut current_bytes = rows.iter().map(|row| row.len()).sum();
        let result = flush_pending(&mut rows, &mut current_bytes, &mut |payload| {
            Box::pin(async move {
                assert_eq!(payload, "{\"a\":1}\n{\"a\":2}");
                Ok(IngestResult {
                    num_rows_inserted: 2,
                })
            })
        })
        .await
        .unwrap();

        assert_eq!(result.unwrap().num_rows_inserted, 2);
        assert!(rows.is_empty());
        assert_eq!(current_bytes, 0);
    }

    #[tokio::test]
    async fn test_flush_pending_empty_batch() {
        let mut rows = Vec::new();
        let mut current_bytes = 0;
        let result = flush_pending(&mut rows, &mut current_bytes, &mut |_payload| {
            Box::pin(async move { Err(test_error("must not be called")) })
        })
        .await
        .unwrap();

        assert!(result.is_none());
    }
}
