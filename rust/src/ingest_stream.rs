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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use mea::mpsc;
use mea::mutex::Mutex;
use mea::oneshot;
use mea::semaphore::OwnedSemaphorePermit;
use mea::semaphore::Semaphore;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::Client;
use crate::Error;
use crate::ErrorKind;
use crate::IngestResult;

const DEFAULT_BATCH_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_MAX_PENDING_BYTES: usize = DEFAULT_BATCH_BYTES * 4;
const DEFAULT_MAX_RETRIES: usize = 8;
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FatalStatus {
    Permanent,
    Temporary,
    Persistent,
}

#[derive(Debug, Clone)]
struct FatalState {
    message: String,
    status: FatalStatus,
}

impl FatalState {
    fn from_error(error: &Error) -> Self {
        let status = if error.is_temporary() {
            FatalStatus::Temporary
        } else if error.is_persistent() {
            FatalStatus::Persistent
        } else {
            FatalStatus::Permanent
        };

        Self {
            message: error.to_string(),
            status,
        }
    }

    fn into_error(self) -> Error {
        let error = Error::new(ErrorKind::Unexpected, self.message);
        match self.status {
            FatalStatus::Temporary => error.set_temporary(),
            FatalStatus::Persistent => error.set_persistent(),
            FatalStatus::Permanent => error.set_permanent(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RetryConfig {
    max_retries: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

#[derive(Debug)]
struct BufferedRecord {
    payload: String,
    _reservation: PendingBytesReservation,
}

enum BatchCommand {
    Record(BufferedRecord),
    Flush(oneshot::Sender<Result<Option<IngestResult>, Error>>),
    Shutdown(oneshot::Sender<Result<Option<IngestResult>, Error>>),
}

#[derive(Debug)]
struct PendingBytesBudget {
    capacity: usize,
    semaphore: Arc<Semaphore>,
    closed: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingBytesAcquireError {
    Closed,
    ExceedsCapacity { requested: usize, capacity: usize },
}

type PendingBytesReservation = OwnedSemaphorePermit;

impl PendingBytesBudget {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1).min(u32::MAX as usize);
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
            closed: AtomicBool::new(false),
        }
    }

    async fn acquire(
        &self,
        requested: usize,
    ) -> Result<PendingBytesReservation, PendingBytesAcquireError> {
        if requested > self.capacity {
            return Err(PendingBytesAcquireError::ExceedsCapacity {
                requested,
                capacity: self.capacity,
            });
        }

        if self.closed.load(Ordering::Acquire) {
            return Err(PendingBytesAcquireError::Closed);
        }

        let reservation = self.semaphore.clone().acquire_owned(requested).await;
        if self.closed.load(Ordering::Acquire) {
            drop(reservation);
            return Err(PendingBytesAcquireError::Closed);
        }

        Ok(reservation)
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.semaphore.release(self.capacity);
        }
    }
}

impl IngestStreamBuilder {
    pub(crate) fn new(client: Client, statement: String) -> Self {
        Self {
            client,
            statement,
            batch_bytes: DEFAULT_BATCH_BYTES,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            retry: RetryConfig {
                max_retries: DEFAULT_MAX_RETRIES,
                initial_backoff: DEFAULT_INITIAL_BACKOFF,
                max_backoff: DEFAULT_MAX_BACKOFF,
            },
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

    pub fn max_pending_bytes(mut self, max_pending_bytes: usize) -> Self {
        self.max_pending_bytes = max_pending_bytes.max(1);
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.retry.max_retries = max_retries;
        self
    }

    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.retry.initial_backoff = initial_backoff;
        self
    }

    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.retry.max_backoff = max_backoff;
        self
    }

    pub fn build(self) -> IngestStream {
        IngestStream::new(
            self.client,
            self.statement,
            self.batch_bytes,
            self.flush_interval,
            self.channel_capacity,
            self.max_pending_bytes,
            self.retry,
        )
    }
}

pub struct IngestStreamBuilder {
    client: Client,
    statement: String,
    batch_bytes: usize,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    retry: RetryConfig,
}

pub struct IngestStream {
    tx: mpsc::BoundedSender<BatchCommand>,
    task: Mutex<Option<JoinHandle<()>>>,
    fatal: Arc<Mutex<Option<FatalState>>>,
    pending_bytes: Arc<PendingBytesBudget>,
}

impl IngestStream {
    fn new(
        client: Client,
        statement: String,
        batch_bytes: usize,
        flush_interval: Duration,
        channel_capacity: usize,
        max_pending_bytes: usize,
        retry: RetryConfig,
    ) -> Self {
        let (tx, rx) = mpsc::bounded(channel_capacity.max(1));
        let fatal = Arc::new(Mutex::new(None));
        let pending_bytes = Arc::new(PendingBytesBudget::new(max_pending_bytes.max(1)));
        let task = tokio::spawn(run_batch_worker(
            rx,
            batch_bytes,
            flush_interval,
            retry,
            fatal.clone(),
            pending_bytes.clone(),
            move |rows| {
                let client = client.clone();
                let statement = statement.clone();
                Box::pin(async move { client.insert(rows, statement).await })
            },
        ));

        Self {
            tx,
            task: Mutex::new(Some(task)),
            fatal,
            pending_bytes,
        }
    }

    pub async fn send<T: Serialize>(&self, record: &T) -> Result<(), Error> {
        self.check_fatal().await?;
        let payload = serde_json::to_string(record).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to serialize batched ingest record".to_string(),
            )
            .set_source(err)
        })?;

        let reserved = match self.pending_bytes.acquire(buffered_bytes(&payload)).await {
            Ok(reserved) => reserved,
            Err(PendingBytesAcquireError::Closed) => {
                return Err(self.closed_or_fatal_error().await);
            }
            Err(PendingBytesAcquireError::ExceedsCapacity {
                requested,
                capacity,
            }) => {
                return Err(
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "ingest stream record requires {requested} buffered bytes, exceeds max_pending_bytes={capacity}"
                        ),
                    )
                    .set_permanent(),
                );
            }
        };

        if self
            .tx
            .send(BatchCommand::Record(BufferedRecord {
                payload,
                _reservation: reserved,
            }))
            .await
            .is_err()
        {
            return Err(self.closed_or_fatal_error().await);
        }
        self.check_fatal().await
    }

    pub async fn flush(&self) -> Result<Option<IngestResult>, Error> {
        self.check_fatal().await?;
        let (tx, rx) = oneshot::channel();
        if self.tx.send(BatchCommand::Flush(tx)).await.is_err() {
            return Err(self.closed_or_fatal_error().await);
        }
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(self.closed_or_fatal_error().await),
        }
    }

    pub async fn shutdown(self) -> Result<Option<IngestResult>, Error> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(BatchCommand::Shutdown(tx)).await.is_err() {
            return Err(self.closed_or_fatal_error().await);
        }

        let result = match rx.await {
            Ok(result) => result,
            Err(_) => return Err(self.closed_or_fatal_error().await),
        };
        let task = self.task.lock().await.take();
        if let Some(task) = task {
            task.await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ingest stream background task panicked".to_string(),
                )
                .set_source(err)
            })?;
        }
        result
    }

    async fn check_fatal(&self) -> Result<(), Error> {
        if let Some(state) = self.fatal.lock().await.clone() {
            Err(state.into_error())
        } else {
            Ok(())
        }
    }

    async fn closed_or_fatal_error(&self) -> Error {
        self.fatal
            .lock()
            .await
            .clone()
            .map(FatalState::into_error)
            .unwrap_or_else(|| {
                Error::new(ErrorKind::Unexpected, "ingest stream is closed".to_string())
                    .set_persistent()
            })
    }
}

type BoxFutureIngest = Pin<Box<dyn Future<Output = Result<IngestResult, Error>> + Send>>;

async fn run_batch_worker<F>(
    mut rx: mpsc::BoundedReceiver<BatchCommand>,
    batch_bytes: usize,
    flush_interval: Duration,
    retry: RetryConfig,
    fatal: Arc<Mutex<Option<FatalState>>>,
    pending_bytes: Arc<PendingBytesBudget>,
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
                if let Err(err) = flush_pending(&mut rows, &mut current_bytes, retry, &mut flush_fn).await {
                    *fatal.lock().await = Some(FatalState::from_error(&err));
                    break;
                }
            }
            command = rx.recv() => {
                let Ok(command) = command else {
                    if let Err(err) = flush_pending(&mut rows, &mut current_bytes, retry, &mut flush_fn).await {
                        *fatal.lock().await = Some(FatalState::from_error(&err));
                    }
                    break;
                };

                match command {
                    BatchCommand::Record(record) => {
                        if !rows.is_empty() {
                            current_bytes = current_bytes.saturating_add(1);
                        }
                        current_bytes = current_bytes.saturating_add(record.payload.len());
                        rows.push(record);
                        if !rows.is_empty() && current_bytes >= batch_bytes {
                            if let Err(err) = flush_pending(&mut rows, &mut current_bytes, retry, &mut flush_fn).await {
                                *fatal.lock().await = Some(FatalState::from_error(&err));
                                break;
                            }
                        }
                    }
                    BatchCommand::Flush(ack) => {
                        let result = flush_pending(&mut rows, &mut current_bytes, retry, &mut flush_fn).await;
                        if let Err(err) = &result {
                            *fatal.lock().await = Some(FatalState::from_error(err));
                        }
                        let _ = ack.send(result);
                        if fatal.lock().await.is_some() {
                            break;
                        }
                    }
                    BatchCommand::Shutdown(ack) => {
                        let result = flush_pending(&mut rows, &mut current_bytes, retry, &mut flush_fn).await;
                        if let Err(err) = &result {
                            *fatal.lock().await = Some(FatalState::from_error(err));
                        }
                        let _ = ack.send(result);
                        break;
                    }
                }
            }
        }
    }

    pending_bytes.close();
}

async fn flush_pending<F>(
    rows: &mut Vec<BufferedRecord>,
    current_bytes: &mut usize,
    retry: RetryConfig,
    flush_fn: &mut F,
) -> Result<Option<IngestResult>, Error>
where
    F: FnMut(String) -> BoxFutureIngest,
{
    if rows.is_empty() {
        return Ok(None);
    }

    let payload = rows
        .iter()
        .map(|row| row.payload.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let mut retries = 0usize;
    let mut backoff = retry.initial_backoff;

    loop {
        match flush_fn(payload.clone()).await {
            Ok(result) => {
                rows.clear();
                *current_bytes = 0;
                return Ok(Some(result));
            }
            Err(err) if err.is_temporary() && retries < retry.max_retries => {
                retries += 1;
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
                backoff = next_backoff(backoff, retry.max_backoff);
            }
            Err(err) if err.is_temporary() => return Err(retry_exhausted_error(retries, err)),
            Err(err) => return Err(err),
        }
    }
}

fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }

    match current.checked_mul(2) {
        Some(next) => next.min(max_backoff),
        None => max_backoff,
    }
}

fn retry_exhausted_error(retries: usize, err: Error) -> Error {
    let last_error = err.to_string();
    Error::new(
        ErrorKind::Unexpected,
        "ingest stream flush exhausted retry budget".to_string(),
    )
    .with_context("retries", retries)
    .with_context("last_error", last_error)
    .set_source(err)
    .set_persistent()
}

fn buffered_bytes(payload: &str) -> usize {
    payload.len().saturating_add(1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    fn test_error(message: &str) -> Error {
        Error::new(ErrorKind::Unexpected, message.to_string())
    }

    fn temporary_error(message: &str) -> Error {
        test_error(message).set_temporary()
    }

    async fn test_record(payload: &str) -> BufferedRecord {
        let budget = Arc::new(PendingBytesBudget::new(1024));
        let reservation = budget
            .acquire(buffered_bytes(payload))
            .await
            .expect("reservation should succeed");
        BufferedRecord {
            payload: payload.to_string(),
            _reservation: reservation,
        }
    }

    fn test_retry() -> RetryConfig {
        RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn test_flush_pending_joins_rows() {
        let mut rows = vec![
            test_record("{\"a\":1}").await,
            test_record("{\"a\":2}").await,
        ];
        let mut current_bytes = rows.iter().map(|row| row.payload.len()).sum::<usize>() + 1;
        let result = flush_pending(
            &mut rows,
            &mut current_bytes,
            test_retry(),
            &mut |payload| {
                Box::pin(async move {
                    assert_eq!(payload, "{\"a\":1}\n{\"a\":2}");
                    Ok(IngestResult {
                        num_rows_inserted: 2,
                    })
                })
            },
        )
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
        let result = flush_pending(
            &mut rows,
            &mut current_bytes,
            test_retry(),
            &mut |_payload| Box::pin(async move { Err(test_error("must not be called")) }),
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_flush_pending_keeps_batch_on_error() {
        let mut rows = vec![
            test_record("{\"a\":1}").await,
            test_record("{\"a\":2}").await,
        ];
        let original_rows = rows
            .iter()
            .map(|row| row.payload.clone())
            .collect::<Vec<_>>();
        let mut current_bytes = rows.iter().map(|row| row.payload.len()).sum::<usize>() + 1;

        let err = flush_pending(
            &mut rows,
            &mut current_bytes,
            test_retry(),
            &mut |_payload| Box::pin(async move { Err(test_error("flush failed")) }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), test_error("flush failed").to_string());
        assert_eq!(
            rows.iter()
                .map(|row| row.payload.as_str())
                .collect::<Vec<_>>(),
            original_rows
                .iter()
                .map(|row| row.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            current_bytes,
            original_rows.iter().map(|row| row.len()).sum::<usize>() + 1
        );
    }

    #[tokio::test]
    async fn test_flush_pending_retries_temporary_errors() {
        let mut rows = vec![test_record("{\"a\":1}").await];
        let mut current_bytes = rows[0].payload.len();
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = flush_pending(&mut rows, &mut current_bytes, test_retry(), &mut {
            let attempts = attempts.clone();
            move |_payload| {
                let attempts = attempts.clone();
                Box::pin(async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err(temporary_error("retry me"))
                    } else {
                        Ok(IngestResult {
                            num_rows_inserted: 1,
                        })
                    }
                })
            }
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(result.num_rows_inserted, 1);
        assert!(rows.is_empty());
        assert_eq!(current_bytes, 0);
    }

    #[tokio::test]
    async fn test_flush_pending_exhausts_retry_budget() {
        let mut rows = vec![test_record("{\"a\":1}").await];
        let mut current_bytes = rows[0].payload.len();

        let err = flush_pending(
            &mut rows,
            &mut current_bytes,
            test_retry(),
            &mut |_payload| Box::pin(async move { Err(temporary_error("still rate limited")) }),
        )
        .await
        .unwrap_err();

        assert!(err.is_persistent());
        assert!(err.to_string().contains("retry budget"));
        assert_eq!(rows.len(), 1);
        assert_eq!(current_bytes, rows[0].payload.len());
    }

    #[tokio::test]
    async fn test_pending_bytes_budget_blocks_until_release() {
        let budget = Arc::new(PendingBytesBudget::new(8));
        let reservation = budget.acquire(8).await.unwrap();

        let waiter = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(1).await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());

        drop(reservation);

        let reservation = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(reservation);
    }

    #[tokio::test]
    async fn test_pending_bytes_budget_close_unblocks_waiters() {
        let budget = Arc::new(PendingBytesBudget::new(8));
        let reservation = budget.acquire(8).await.unwrap();

        let waiter = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(1).await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        budget.close();
        drop(reservation);

        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(PendingBytesAcquireError::Closed)));
    }
}
