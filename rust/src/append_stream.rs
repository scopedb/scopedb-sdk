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

use std::error::Error as StdError;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

use crate::AppendErrorDetails;
use crate::AppendRowsResult;
use crate::AppendState;
use crate::Client;
use crate::Error;
use crate::ErrorKind;

const MAX_APPEND_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_APPEND_ROWS: usize = 200_000;
const DEFAULT_BATCH_BYTES: usize = MAX_APPEND_BODY_BYTES;
const DEFAULT_MAX_BATCH_ROWS: usize = MAX_APPEND_ROWS;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 4;
const DEFAULT_MAX_PENDING_BYTES: usize = DEFAULT_BATCH_BYTES * DEFAULT_MAX_IN_FLIGHT_REQUESTS;
const DEFAULT_MAX_RETRIES: usize = 8;
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_CONTINUE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD: usize = 5;
const DEFAULT_CIRCUIT_COOLDOWN: Duration = Duration::from_secs(30);

/// Determines what an append stream does after one HTTP batch fails.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendFailurePolicy {
    /// Stop accepting rows and fail the stream's delivery barrier.
    #[default]
    Stop,
    /// Record the loss in the delivery report and continue with later batches.
    Continue,
}

/// The lifecycle state of an append stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendStreamState {
    Open,
    Closing,
    Closed,
    Failed,
}

/// The state of the continue-mode availability circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendCircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// The aggregate result of a delivery barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendDeliveryOutcome {
    /// Every covered row was committed and no local rows were dropped.
    Ok,
    /// Some covered rows committed and some were lost or have an unknown outcome.
    Partial,
    /// No covered row committed and all losses are known not to have committed.
    Failed,
    /// No covered row is known to have committed and at least one may have committed.
    Unknown,
}

/// Rows accepted by a bulk local-admission operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAdmissionResult {
    pub accepted_rows: u64,
}

/// Aggregate settlement information since the previous delivery barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendDeliveryReport {
    pub outcome: AppendDeliveryOutcome,
    pub accepted_rows: u64,
    pub committed_rows: u64,
    pub failed_rows: u64,
    pub unknown_rows: u64,
    pub dropped_rows: u64,
    pub committed_batches: u64,
    pub failed_batches: u64,
    pub unknown_batches: u64,
    pub retries: u64,
    /// Wall time spent waiting for this barrier.
    pub duration: Duration,
}

/// Local drop counters grouped by admission failure reason.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AppendDroppedRows {
    pub buffer_full: u64,
    pub invalid_record: u64,
    pub record_too_large: u64,
    pub circuit_open: u64,
    pub closed: u64,
}

/// The most recent remote append failure observed by the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendLastFailure {
    pub at: SystemTime,
    pub message: String,
    pub append_state: Option<AppendState>,
}

/// A point-in-time snapshot of append stream counters and state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendStreamStats {
    pub state: AppendStreamState,
    pub circuit_state: AppendCircuitState,
    pub accepted_rows: u64,
    pub committed_rows: u64,
    pub failed_rows: u64,
    pub unknown_rows: u64,
    pub dropped_rows: u64,
    pub dropped_by_reason: AppendDroppedRows,
    pub retries: u64,
    pub pending_rows: u64,
    pub pending_bytes: usize,
    pub in_flight_batches: usize,
    pub last_failure: Option<AppendLastFailure>,
    pub last_report: Option<AppendDeliveryReport>,
}

/// Action taken after a batch failure listener is invoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppendBatchFailureAction {
    Continuing,
    CircuitOpened,
    Stopped,
}

/// Diagnostic event emitted after a rejected or ambiguous append batch.
pub struct AppendBatchFailureEvent {
    pub error: Error,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub outcome: AppendState,
    pub action: AppendBatchFailureAction,
}

impl fmt::Debug for AppendBatchFailureEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendBatchFailureEvent")
            .field("error", &self.error)
            .field("batch_rows", &self.batch_rows)
            .field("batch_bytes", &self.batch_bytes)
            .field("outcome", &self.outcome)
            .field("action", &self.action)
            .finish()
    }
}

/// Continue-mode circuit breaker settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct AppendCircuitBreakerOptions {
    pub failure_threshold: usize,
    pub cooldown: Duration,
}

impl AppendCircuitBreakerOptions {
    pub fn new(failure_threshold: usize, cooldown: Duration) -> Self {
        Self {
            failure_threshold,
            cooldown,
        }
    }
}

impl Default for AppendCircuitBreakerOptions {
    fn default() -> Self {
        Self::new(DEFAULT_CIRCUIT_FAILURE_THRESHOLD, DEFAULT_CIRCUIT_COOLDOWN)
    }
}

/// A synchronous local-admission failure from [`AppendStream::try_send`].
#[derive(Debug)]
#[non_exhaustive]
pub enum AppendTrySendError {
    Serialization(serde_json::Error),
    InvalidRecord,
    RecordTooLarge { bytes: usize, limit: usize },
    BufferFull,
    CircuitOpen,
    Closed,
}

impl fmt::Display for AppendTrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(err) => write!(f, "failed to serialize append row: {err}"),
            Self::InvalidRecord => write!(f, "append row must serialize to a JSON object"),
            Self::RecordTooLarge { bytes, limit } => {
                write!(
                    f,
                    "append row is {bytes} bytes, exceeding the {limit}-byte limit"
                )
            }
            Self::BufferFull => write!(f, "append stream local buffer is full"),
            Self::CircuitOpen => write!(f, "append stream circuit breaker is open"),
            Self::Closed => write!(f, "append stream is closed"),
        }
    }
}

impl StdError for AppendTrySendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Serialization(err) => Some(err),
            _ => None,
        }
    }
}

type BatchFailureListener = Arc<dyn Fn(&AppendBatchFailureEvent) + Send + Sync>;

#[derive(Debug, Clone, Copy)]
struct RetryConfig {
    max_retries: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
}

/// Configures a bounded, concurrent table append stream.
pub struct AppendStreamBuilder {
    client: Client,
    database: String,
    schema: String,
    table: String,
    batch_bytes: usize,
    max_batch_rows: usize,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    max_in_flight_requests: usize,
    retry: RetryConfig,
    failure_policy: AppendFailurePolicy,
    attempt_timeout: Option<Duration>,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
}

impl AppendStreamBuilder {
    pub(crate) fn new(client: Client, database: String, schema: String, table: String) -> Self {
        Self {
            client,
            database,
            schema,
            table,
            batch_bytes: DEFAULT_BATCH_BYTES,
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            retry: RetryConfig {
                max_retries: DEFAULT_MAX_RETRIES,
                initial_backoff: DEFAULT_INITIAL_BACKOFF,
                max_backoff: DEFAULT_MAX_BACKOFF,
            },
            failure_policy: AppendFailurePolicy::Stop,
            attempt_timeout: None,
            circuit_breaker: Some(AppendCircuitBreakerOptions::default()),
            batch_failure_listeners: Vec::new(),
        }
    }

    pub fn failure_policy(mut self, failure_policy: AppendFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Sets the target NDJSON payload size. One row may exceed this target up to 16 MiB.
    #[deprecated(note = "use target_batch_bytes")]
    pub fn batch_bytes(mut self, batch_bytes: usize) -> Self {
        self.batch_bytes = batch_bytes;
        self
    }

    /// Sets the target NDJSON payload size. One row may exceed this target up to 16 MiB.
    pub fn target_batch_bytes(mut self, target_batch_bytes: usize) -> Self {
        self.batch_bytes = target_batch_bytes;
        self
    }

    /// Sets the maximum rows in one HTTP batch, up to the 200,000-row protocol limit.
    pub fn max_batch_rows(mut self, max_batch_rows: usize) -> Self {
        self.max_batch_rows = max_batch_rows;
        self
    }

    /// Sets the maximum delay between buffering the first row and dispatching its batch.
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    pub fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity;
        self
    }

    /// Backward-compatible alias for [`AppendStreamBuilder::max_buffered_bytes`].
    #[deprecated(note = "use max_buffered_bytes")]
    pub fn max_pending_bytes(mut self, max_pending_bytes: usize) -> Self {
        self.max_pending_bytes = max_pending_bytes;
        self
    }

    /// Sets the maximum bytes admitted locally but not yet settled remotely.
    pub fn max_buffered_bytes(mut self, max_buffered_bytes: usize) -> Self {
        self.max_pending_bytes = max_buffered_bytes;
        self
    }

    /// Sets the maximum number of append HTTP requests running concurrently.
    #[deprecated(note = "use max_concurrent_batches")]
    pub fn max_in_flight_requests(mut self, max_in_flight_requests: usize) -> Self {
        self.max_in_flight_requests = max_in_flight_requests;
        self
    }

    /// Sets the maximum number of append batches sent concurrently.
    pub fn max_concurrent_batches(mut self, max_concurrent_batches: usize) -> Self {
        self.max_in_flight_requests = max_concurrent_batches;
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

    /// Sets a timeout for each HTTP attempt. A timeout has an unknown commit outcome.
    pub fn attempt_timeout(mut self, attempt_timeout: Duration) -> Self {
        self.attempt_timeout = Some(attempt_timeout);
        self
    }

    /// Configures the continue-mode availability circuit, or disables it with `None`.
    pub fn circuit_breaker(mut self, circuit_breaker: Option<AppendCircuitBreakerOptions>) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    /// Observes remote batch failures. Listener panics are isolated from the worker.
    pub fn on_batch_failure<F>(mut self, listener: F) -> Self
    where
        F: Fn(&AppendBatchFailureEvent) + Send + Sync + 'static,
    {
        self.batch_failure_listeners.push(Arc::new(listener));
        self
    }

    pub fn build(self) -> Result<AppendStream, Error> {
        validate_builder(&self)?;
        tokio::runtime::Handle::try_current().map_err(|err| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "append stream must be built inside a Tokio runtime",
            )
            .set_source(err)
        })?;

        let attempt_timeout = self.attempt_timeout.or_else(|| {
            (self.failure_policy == AppendFailurePolicy::Continue)
                .then_some(DEFAULT_CONTINUE_ATTEMPT_TIMEOUT)
        });
        Ok(AppendStream::new(AppendStreamConfig {
            client: self.client,
            database: self.database,
            schema: self.schema,
            table: self.table,
            batch_bytes: self.batch_bytes,
            max_batch_rows: self.max_batch_rows,
            flush_interval: self.flush_interval,
            channel_capacity: self.channel_capacity,
            max_pending_bytes: self.max_pending_bytes,
            max_in_flight_requests: self.max_in_flight_requests,
            retry: self.retry,
            failure_policy: self.failure_policy,
            attempt_timeout,
            circuit_breaker: self.circuit_breaker,
            batch_failure_listeners: self.batch_failure_listeners,
        }))
    }
}

fn validate_builder(builder: &AppendStreamBuilder) -> Result<(), Error> {
    validate_positive("target_batch_bytes", builder.batch_bytes)?;
    if builder.batch_bytes > MAX_APPEND_BODY_BYTES {
        return Err(config_error(format!(
            "target_batch_bytes must not exceed {MAX_APPEND_BODY_BYTES}"
        )));
    }
    validate_positive("max_batch_rows", builder.max_batch_rows)?;
    if builder.max_batch_rows > MAX_APPEND_ROWS {
        return Err(config_error(format!(
            "max_batch_rows must not exceed {MAX_APPEND_ROWS}"
        )));
    }
    if builder.flush_interval.is_zero() {
        return Err(config_error("flush_interval must be greater than zero"));
    }
    validate_deadline("flush_interval", builder.flush_interval)?;
    validate_positive("channel_capacity", builder.channel_capacity)?;
    validate_positive("max_buffered_bytes", builder.max_pending_bytes)?;
    if builder.max_pending_bytes > u32::MAX as usize {
        return Err(config_error(format!(
            "max_buffered_bytes must not exceed {}",
            u32::MAX
        )));
    }
    validate_positive("max_concurrent_batches", builder.max_in_flight_requests)?;
    if builder
        .attempt_timeout
        .is_some_and(|attempt_timeout| attempt_timeout.is_zero())
    {
        return Err(config_error("attempt_timeout must be greater than zero"));
    }
    if let Some(attempt_timeout) = builder.attempt_timeout {
        validate_deadline("attempt_timeout", attempt_timeout)?;
    }
    if let Some(circuit) = builder.circuit_breaker {
        validate_positive(
            "circuit_breaker.failure_threshold",
            circuit.failure_threshold,
        )?;
        if circuit.cooldown.is_zero() {
            return Err(config_error(
                "circuit_breaker.cooldown must be greater than zero",
            ));
        }
        validate_deadline("circuit_breaker.cooldown", circuit.cooldown)?;
    }
    Ok(())
}

fn validate_deadline(name: &str, duration: Duration) -> Result<(), Error> {
    if Instant::now().checked_add(duration).is_none() {
        Err(config_error(format!("{name} is too large")))
    } else {
        Ok(())
    }
}

fn validate_positive(name: &str, value: usize) -> Result<(), Error> {
    if value == 0 {
        Err(config_error(format!("{name} must be greater than zero")))
    } else {
        Ok(())
    }
}

fn config_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message)
}

struct AppendStreamConfig {
    client: Client,
    database: String,
    schema: String,
    table: String,
    batch_bytes: usize,
    max_batch_rows: usize,
    flush_interval: Duration,
    channel_capacity: usize,
    max_pending_bytes: usize,
    max_in_flight_requests: usize,
    retry: RetryConfig,
    failure_policy: AppendFailurePolicy,
    attempt_timeout: Option<Duration>,
    circuit_breaker: Option<AppendCircuitBreakerOptions>,
    batch_failure_listeners: Vec<BatchFailureListener>,
}

enum AppendCommand {
    Record(BufferedRecord),
    Flush(BarrierCommand),
    Shutdown(BarrierCommand),
}

struct BarrierCommand {
    dropped_rows_at_barrier: u64,
    started_at: Instant,
    ack: oneshot::Sender<Result<AppendDeliveryReport, Error>>,
}

struct BufferedRecord {
    payload: String,
    reserved_bytes: usize,
    _reservation: OwnedSemaphorePermit,
}

struct PendingBytesBudget {
    capacity: usize,
    semaphore: Arc<Semaphore>,
    closed: AtomicBool,
}

impl PendingBytesBudget {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            semaphore: Arc::new(Semaphore::new(capacity)),
            closed: AtomicBool::new(false),
        }
    }

    async fn acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingAcquireError::Closed);
        }
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| PendingAcquireError::Closed)?;
        if self.closed.load(Ordering::Acquire) {
            drop(permit);
            return Err(PendingAcquireError::Closed);
        }
        Ok(permit)
    }

    fn try_acquire(&self, bytes: usize) -> Result<OwnedSemaphorePermit, PendingAcquireError> {
        if bytes > self.capacity {
            return Err(PendingAcquireError::ExceedsCapacity);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(PendingAcquireError::Closed);
        }
        self.semaphore
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|err| match err {
                tokio::sync::TryAcquireError::Closed => PendingAcquireError::Closed,
                tokio::sync::TryAcquireError::NoPermits => PendingAcquireError::Full,
            })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.semaphore.close();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingAcquireError {
    Closed,
    Full,
    ExceedsCapacity,
}

#[derive(Debug, Clone, Copy, Default)]
struct DeliveryCounters {
    accepted_rows: u64,
    committed_rows: u64,
    failed_rows: u64,
    unknown_rows: u64,
    committed_batches: u64,
    failed_batches: u64,
    unknown_batches: u64,
    retries: u64,
}

struct CircuitRuntime {
    state: AppendCircuitState,
    opened_until: Option<Instant>,
    consecutive_failures: usize,
    probe_admission_claimed: bool,
}

impl Default for CircuitRuntime {
    fn default() -> Self {
        Self {
            state: AppendCircuitState::Closed,
            opened_until: None,
            consecutive_failures: 0,
            probe_admission_claimed: false,
        }
    }
}

struct SharedState {
    lifecycle: AppendStreamState,
    fatal: Option<StreamErrorSnapshot>,
    final_result: Option<Result<AppendDeliveryReport, StreamErrorSnapshot>>,
    accepted_rows: u64,
    committed_rows: u64,
    failed_rows: u64,
    unknown_rows: u64,
    dropped_rows: u64,
    dropped_by_reason: AppendDroppedRows,
    retries: u64,
    pending_rows: u64,
    pending_bytes: usize,
    in_flight_batches: usize,
    last_failure: Option<AppendLastFailure>,
    last_report: Option<AppendDeliveryReport>,
    circuit: CircuitRuntime,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            lifecycle: AppendStreamState::Open,
            fatal: None,
            final_result: None,
            accepted_rows: 0,
            committed_rows: 0,
            failed_rows: 0,
            unknown_rows: 0,
            dropped_rows: 0,
            dropped_by_reason: AppendDroppedRows::default(),
            retries: 0,
            pending_rows: 0,
            pending_bytes: 0,
            in_flight_batches: 0,
            last_failure: None,
            last_report: None,
            circuit: CircuitRuntime::default(),
        }
    }
}

struct AppendStreamInner {
    tx: mpsc::Sender<AppendCommand>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
}

/// A cloneable, bounded producer handle for asynchronous table appends.
#[derive(Clone)]
pub struct AppendStream {
    inner: Arc<AppendStreamInner>,
}

impl AppendStream {
    fn new(config: AppendStreamConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let pending_bytes = Arc::new(PendingBytesBudget::new(config.max_pending_bytes));
        let shutdown_notify = Arc::new(Notify::new());

        let worker = tokio::spawn(run_append_worker(
            config,
            rx,
            shared.clone(),
            pending_bytes.clone(),
            shutdown_notify.clone(),
        ));
        tokio::spawn(monitor_append_worker(
            worker,
            shared.clone(),
            pending_bytes.clone(),
            shutdown_notify.clone(),
        ));

        Self {
            inner: Arc::new(AppendStreamInner {
                tx,
                shared,
                pending_bytes,
                shutdown_notify,
            }),
        }
    }

    /// Serializes and admits one row, waiting for bounded local capacity.
    ///
    /// Success means local admission only. Use [`Self::flush`] or
    /// [`Self::shutdown`] to wait for remote settlement.
    pub async fn send<T>(&self, row: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.check_open()?;
        let payload = prepare_record(row)?;
        let reserved_bytes = payload.len().saturating_add(1);
        let reservation = self
            .inner
            .pending_bytes
            .acquire(reserved_bytes)
            .await
            .map_err(|err| self.map_pending_error(err, reserved_bytes))?;
        let permit = match self.inner.tx.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                drop(reservation);
                return Err(self.closed_or_fatal_error());
            }
        };

        let mut shared = lock(&self.inner.shared);
        if shared.lifecycle != AppendStreamState::Open {
            drop(shared);
            drop(permit);
            drop(reservation);
            return Err(self.closed_or_fatal_error());
        }
        shared.accepted_rows = shared.accepted_rows.saturating_add(1);
        shared.pending_rows = shared.pending_rows.saturating_add(1);
        shared.pending_bytes = shared.pending_bytes.saturating_add(reserved_bytes);
        permit.send(AppendCommand::Record(BufferedRecord {
            payload,
            reserved_bytes,
            _reservation: reservation,
        }));
        Ok(())
    }

    /// Attempts synchronous local admission without waiting for capacity.
    pub fn try_send<T>(&self, row: &T) -> Result<(), AppendTrySendError>
    where
        T: Serialize + ?Sized,
    {
        {
            let shared = lock(&self.inner.shared);
            if shared.lifecycle != AppendStreamState::Open {
                drop(shared);
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
            if !circuit_admission_available(&shared) {
                drop(shared);
                note_drop(&mut lock(&self.inner.shared), DropReason::CircuitOpen);
                return Err(AppendTrySendError::CircuitOpen);
            }
        }

        let payload = match prepare_record_for_try_send(row) {
            Ok(payload) => payload,
            Err(err) => {
                let reason = match err {
                    AppendTrySendError::RecordTooLarge { .. } => DropReason::RecordTooLarge,
                    _ => DropReason::InvalidRecord,
                };
                note_drop(&mut lock(&self.inner.shared), reason);
                return Err(err);
            }
        };
        let reserved_bytes = payload.len().saturating_add(1);
        let reservation = match self.inner.pending_bytes.try_acquire(reserved_bytes) {
            Ok(reservation) => reservation,
            Err(PendingAcquireError::Closed) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
            Err(PendingAcquireError::Full | PendingAcquireError::ExceedsCapacity) => {
                note_drop(&mut lock(&self.inner.shared), DropReason::BufferFull);
                return Err(AppendTrySendError::BufferFull);
            }
        };
        let permit = match self.inner.tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                drop(reservation);
                note_drop(&mut lock(&self.inner.shared), DropReason::BufferFull);
                return Err(AppendTrySendError::BufferFull);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                drop(reservation);
                note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
                return Err(AppendTrySendError::Closed);
            }
        };

        let mut shared = lock(&self.inner.shared);
        if shared.lifecycle != AppendStreamState::Open {
            drop(shared);
            drop(permit);
            drop(reservation);
            note_drop(&mut lock(&self.inner.shared), DropReason::Closed);
            return Err(AppendTrySendError::Closed);
        }
        if !try_claim_circuit_admission(&mut shared) {
            drop(shared);
            drop(permit);
            drop(reservation);
            note_drop(&mut lock(&self.inner.shared), DropReason::CircuitOpen);
            return Err(AppendTrySendError::CircuitOpen);
        }
        shared.accepted_rows = shared.accepted_rows.saturating_add(1);
        shared.pending_rows = shared.pending_rows.saturating_add(1);
        shared.pending_bytes = shared.pending_bytes.saturating_add(reserved_bytes);
        permit.send(AppendCommand::Record(BufferedRecord {
            payload,
            reserved_bytes,
            _reservation: reservation,
        }));
        Ok(())
    }

    /// Admits an iterator of rows sequentially with bounded memory.
    pub async fn send_all<I>(&self, rows: I) -> Result<AppendAdmissionResult, Error>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let mut accepted_rows = 0u64;
        for row in rows {
            self.send(&row).await?;
            accepted_rows = accepted_rows.saturating_add(1);
        }
        Ok(AppendAdmissionResult { accepted_rows })
    }

    /// Dispatches rows admitted before this barrier and waits for their outcomes.
    ///
    /// Once enqueued, dropping this future does not cancel remote settlement.
    /// Keep the future alive to receive its interval report; the latest completed
    /// report also remains observable through [`Self::stats`].
    pub async fn flush(&self) -> Result<AppendDeliveryReport, Error> {
        loop {
            let notified = self.inner.shutdown_notify.notified();
            match self.current_terminal_result() {
                TerminalResult::Open => {}
                TerminalResult::Waiting => {
                    notified.await;
                    continue;
                }
                TerminalResult::Complete(result) => return result,
            }

            let permit = match self.inner.tx.reserve().await {
                Ok(permit) => permit,
                Err(_) => return self.wait_for_terminal().await,
            };
            let (ack, response) = oneshot::channel();
            {
                let shared = lock(&self.inner.shared);
                if shared.lifecycle != AppendStreamState::Open {
                    drop(shared);
                    drop(permit);
                    continue;
                }
                permit.send(AppendCommand::Flush(BarrierCommand {
                    dropped_rows_at_barrier: shared.dropped_rows,
                    started_at: Instant::now(),
                    ack,
                }));
            }
            return match response.await {
                Ok(result) => result,
                Err(_) => self.wait_for_terminal().await,
            };
        }
    }

    /// Permanently closes the stream after settling every admitted row.
    ///
    /// This method is idempotent across cloned producer handles.
    pub async fn shutdown(&self) -> Result<AppendDeliveryReport, Error> {
        let should_start = {
            let mut shared = lock(&self.inner.shared);
            match shared.lifecycle {
                AppendStreamState::Open => {
                    shared.lifecycle = AppendStreamState::Closing;
                    true
                }
                AppendStreamState::Closing | AppendStreamState::Failed => false,
                AppendStreamState::Closed if shared.final_result.is_some() => {
                    return terminal_result(&shared);
                }
                AppendStreamState::Closed => false,
            }
        };

        if should_start {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let result = enqueue_shutdown(inner.clone()).await;
                let mut shared = lock(&inner.shared);
                if shared.final_result.is_none() {
                    match &result {
                        Ok(report) => {
                            shared.lifecycle = AppendStreamState::Closed;
                            shared.last_report = Some(report.clone());
                            shared.final_result = Some(Ok(report.clone()));
                        }
                        Err(error) => {
                            let snapshot = StreamErrorSnapshot::from_error(error);
                            shared.lifecycle = AppendStreamState::Failed;
                            shared.fatal = Some(snapshot.clone());
                            shared.final_result = Some(Err(snapshot));
                        }
                    }
                }
                drop(shared);
                inner.pending_bytes.close();
                inner.shutdown_notify.notify_waiters();
            });
        }

        self.wait_for_terminal().await
    }

    /// Returns a synchronous snapshot suitable for metrics and shutdown diagnostics.
    pub fn stats(&self) -> AppendStreamStats {
        let shared = lock(&self.inner.shared);
        AppendStreamStats {
            state: shared.lifecycle,
            circuit_state: shared.circuit.state,
            accepted_rows: shared.accepted_rows,
            committed_rows: shared.committed_rows,
            failed_rows: shared.failed_rows,
            unknown_rows: shared.unknown_rows,
            dropped_rows: shared.dropped_rows,
            dropped_by_reason: shared.dropped_by_reason,
            retries: shared.retries,
            pending_rows: shared.pending_rows,
            pending_bytes: shared.pending_bytes,
            in_flight_batches: shared.in_flight_batches,
            last_failure: shared.last_failure.clone(),
            last_report: shared.last_report.clone(),
        }
    }

    fn check_open(&self) -> Result<(), Error> {
        let shared = lock(&self.inner.shared);
        if shared.lifecycle == AppendStreamState::Open {
            Ok(())
        } else {
            Err(error_from_shared(&shared))
        }
    }

    fn closed_or_fatal_error(&self) -> Error {
        error_from_shared(&lock(&self.inner.shared))
    }

    fn map_pending_error(&self, error: PendingAcquireError, requested: usize) -> Error {
        match error {
            PendingAcquireError::Closed => self.closed_or_fatal_error(),
            PendingAcquireError::Full => Error::new(
                ErrorKind::Unexpected,
                "append stream local byte budget is full",
            )
            .set_temporary(),
            PendingAcquireError::ExceedsCapacity => Error::new(
                ErrorKind::Unexpected,
                format!(
                    "append row requires {requested} buffered bytes, exceeding max_pending_bytes={}",
                    self.inner.pending_bytes.capacity
                ),
            )
            .set_permanent(),
        }
    }

    fn current_terminal_result(&self) -> TerminalResult {
        let shared = lock(&self.inner.shared);
        match shared.lifecycle {
            AppendStreamState::Open => TerminalResult::Open,
            AppendStreamState::Closing => TerminalResult::Waiting,
            AppendStreamState::Closed | AppendStreamState::Failed
                if shared.final_result.is_none() =>
            {
                TerminalResult::Waiting
            }
            AppendStreamState::Closed | AppendStreamState::Failed => {
                TerminalResult::Complete(terminal_result(&shared))
            }
        }
    }

    async fn wait_for_terminal(&self) -> Result<AppendDeliveryReport, Error> {
        loop {
            let notified = self.inner.shutdown_notify.notified();
            {
                let shared = lock(&self.inner.shared);
                if matches!(
                    shared.lifecycle,
                    AppendStreamState::Closed | AppendStreamState::Failed
                ) && shared.final_result.is_some()
                {
                    return terminal_result(&shared);
                }
            }
            notified.await;
        }
    }
}

enum TerminalResult {
    Open,
    Waiting,
    Complete(Result<AppendDeliveryReport, Error>),
}

async fn enqueue_shutdown(inner: Arc<AppendStreamInner>) -> Result<AppendDeliveryReport, Error> {
    let (ack, response) = oneshot::channel();
    let dropped_rows_at_barrier = lock(&inner.shared).dropped_rows;
    inner
        .tx
        .send(AppendCommand::Shutdown(BarrierCommand {
            dropped_rows_at_barrier,
            started_at: Instant::now(),
            ack,
        }))
        .await
        .map_err(|_| error_from_shared(&lock(&inner.shared)))?;
    response
        .await
        .unwrap_or_else(|_| Err(error_from_shared(&lock(&inner.shared))))
}

fn terminal_result(shared: &SharedState) -> Result<AppendDeliveryReport, Error> {
    match &shared.final_result {
        Some(Ok(report)) => Ok(report.clone()),
        Some(Err(error)) => Err(error.to_error()),
        None => Err(error_from_shared(shared)),
    }
}

fn error_from_shared(shared: &SharedState) -> Error {
    shared
        .fatal
        .as_ref()
        .map(StreamErrorSnapshot::to_error)
        .unwrap_or_else(|| {
            Error::new(ErrorKind::Unexpected, "append stream is closed").set_persistent()
        })
}

fn prepare_record<T>(row: &T) -> Result<String, Error>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "failed to serialize append row").set_source(err)
    })?;
    validate_record_payload(&payload).map_err(|err| match err {
        AppendTrySendError::RecordTooLarge { bytes, limit } => Error::new(
            ErrorKind::Unexpected,
            format!("append row is {bytes} bytes, exceeding the {limit}-byte limit"),
        ),
        _ => Error::new(
            ErrorKind::Unexpected,
            "append row must serialize to a JSON object",
        ),
    })?;
    Ok(payload)
}

fn prepare_record_for_try_send<T>(row: &T) -> Result<String, AppendTrySendError>
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_string(row).map_err(AppendTrySendError::Serialization)?;
    validate_record_payload(&payload)?;
    Ok(payload)
}

fn validate_record_payload(payload: &str) -> Result<(), AppendTrySendError> {
    let bytes = payload.len();
    if bytes > MAX_APPEND_BODY_BYTES {
        return Err(AppendTrySendError::RecordTooLarge {
            bytes,
            limit: MAX_APPEND_BODY_BYTES,
        });
    }
    if !payload.starts_with('{') {
        return Err(AppendTrySendError::InvalidRecord);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SnapshotStatus {
    Permanent,
    Temporary,
    Persistent,
}

#[derive(Debug, Clone)]
struct StreamErrorSnapshot {
    kind: ErrorKind,
    message: String,
    status: SnapshotStatus,
    append_details: Option<AppendErrorDetails>,
    http_status: Option<reqwest::StatusCode>,
    request_id: Option<String>,
    retry_after: Option<Duration>,
}

impl StreamErrorSnapshot {
    fn from_error(error: &Error) -> Self {
        let status = if error.is_temporary() {
            SnapshotStatus::Temporary
        } else if error.is_persistent() {
            SnapshotStatus::Persistent
        } else {
            SnapshotStatus::Permanent
        };
        Self {
            kind: error.kind(),
            message: error.message().to_string(),
            status,
            append_details: error.append_details().cloned(),
            http_status: error.http_status(),
            request_id: error.request_id().map(str::to_string),
            retry_after: error.retry_after(),
        }
    }

    fn to_error(&self) -> Error {
        let mut error = Error::new(self.kind, self.message.clone());
        if let Some(details) = self.append_details.clone() {
            error = error.set_append_details(details);
        }
        if let Some(status) = self.http_status {
            error = error.set_http_status(status);
        }
        if let Some(request_id) = self.request_id.clone() {
            error = error.set_request_id(request_id);
        }
        if let Some(retry_after) = self.retry_after {
            error = error.set_retry_after(retry_after);
        }
        match self.status {
            SnapshotStatus::Permanent => error.set_permanent(),
            SnapshotStatus::Temporary => error.set_temporary(),
            SnapshotStatus::Persistent => error.set_persistent(),
        }
    }

    fn append_state(&self) -> Option<AppendState> {
        self.append_details
            .as_ref()
            .map(|details| details.append_state)
    }
}

struct BatchOutcome {
    records: Vec<BufferedRecord>,
    batch_bytes: usize,
    result: Result<AppendRowsResult, Error>,
    retries: usize,
    circuit_probe: bool,
}

struct AppendBatchRequest {
    client: Client,
    database: String,
    schema: String,
    table: String,
    retry: RetryConfig,
    attempt_timeout: Option<Duration>,
}

struct AppendWorker {
    config: AppendStreamConfig,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
    rows: Vec<BufferedRecord>,
    current_bytes: usize,
    batch_deadline: Option<Instant>,
    in_flight: JoinSet<BatchOutcome>,
    interval: DeliveryCounters,
    reported_dropped_rows: u64,
    fatal: Option<StreamErrorSnapshot>,
}

async fn run_append_worker(
    config: AppendStreamConfig,
    mut rx: mpsc::Receiver<AppendCommand>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
) {
    let mut worker = AppendWorker {
        config,
        shared,
        pending_bytes,
        shutdown_notify,
        rows: Vec::new(),
        current_bytes: 0,
        batch_deadline: None,
        in_flight: JoinSet::new(),
        interval: DeliveryCounters::default(),
        reported_dropped_rows: 0,
        fatal: None,
    };

    loop {
        if worker.fatal.is_some() {
            worker.finish_fatal(&mut rx).await;
            return;
        }

        let deadline = worker
            .batch_deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86400 * 365));
        tokio::select! {
            biased;
            outcome = worker.in_flight.join_next(), if !worker.in_flight.is_empty() => {
                worker.handle_joined(outcome).await;
            }
            _ = tokio::time::sleep_until(deadline.into()), if worker.batch_deadline.is_some() => {
                worker.dispatch_buffered().await;
            }
            command = rx.recv() => {
                match command {
                    Some(AppendCommand::Record(record)) => worker.buffer_record(record).await,
                    Some(AppendCommand::Flush(barrier)) => {
                        worker.complete_barrier(barrier).await;
                    }
                    Some(AppendCommand::Shutdown(barrier)) => {
                        worker.complete_barrier(barrier).await;
                        if worker.fatal.is_some() {
                            worker.finish_fatal(&mut rx).await;
                        } else {
                            worker.finish_closed();
                        }
                        return;
                    }
                    None => {
                        worker.dispatch_buffered().await;
                        worker.drain_in_flight().await;
                        if worker.fatal.is_some() {
                            worker.finish_fatal(&mut rx).await;
                        } else {
                            let dropped = lock(&worker.shared).dropped_rows;
                            let report = worker.take_report(dropped, Instant::now());
                            worker.finish_closed_with_report(report);
                        }
                        return;
                    }
                }
            }
        }
    }
}

async fn monitor_append_worker(
    worker: JoinHandle<()>,
    shared: Arc<Mutex<SharedState>>,
    pending_bytes: Arc<PendingBytesBudget>,
    shutdown_notify: Arc<Notify>,
) {
    let worker_result = worker.await;
    let mut shared = lock(&shared);
    if shared.final_result.is_some() {
        return;
    }

    let error = match worker_result {
        Ok(()) => Error::new(
            ErrorKind::Unexpected,
            "append stream worker stopped before publishing a final result",
        )
        .set_persistent(),
        Err(error) => {
            unknown_append_error("append stream worker terminated unexpectedly", Some(error))
        }
    };
    let snapshot = StreamErrorSnapshot::from_error(&error);
    let resolved = shared
        .committed_rows
        .saturating_add(shared.failed_rows)
        .saturating_add(shared.unknown_rows);
    let unresolved = shared.accepted_rows.saturating_sub(resolved);
    shared.unknown_rows = shared.unknown_rows.saturating_add(unresolved);
    shared.pending_rows = 0;
    shared.pending_bytes = 0;
    shared.in_flight_batches = 0;
    shared.lifecycle = AppendStreamState::Failed;
    shared.fatal = Some(snapshot.clone());
    shared.final_result = Some(Err(snapshot));
    drop(shared);
    pending_bytes.close();
    shutdown_notify.notify_waiters();
}

impl AppendWorker {
    async fn buffer_record(&mut self, record: BufferedRecord) {
        self.interval.accepted_rows = self.interval.accepted_rows.saturating_add(1);
        let separator = usize::from(!self.rows.is_empty());
        if !self.rows.is_empty()
            && self
                .current_bytes
                .saturating_add(separator)
                .saturating_add(record.payload.len())
                > self.config.batch_bytes
        {
            self.dispatch_buffered().await;
        }
        if self.fatal.is_some() {
            self.mark_records_failed(vec![record]);
            return;
        }

        if self.rows.is_empty() {
            self.batch_deadline = Some(deadline_after(self.config.flush_interval));
        } else {
            self.current_bytes = self.current_bytes.saturating_add(1);
        }
        self.current_bytes = self.current_bytes.saturating_add(record.payload.len());
        self.rows.push(record);
        if self.current_bytes >= self.config.batch_bytes
            || self.rows.len() >= self.config.max_batch_rows
        {
            self.dispatch_buffered().await;
        }
    }

    async fn dispatch_buffered(&mut self) {
        if self.rows.is_empty() || self.fatal.is_some() {
            return;
        }
        while self.in_flight.len() >= self.config.max_in_flight_requests {
            let outcome = self.in_flight.join_next().await;
            self.handle_joined(outcome).await;
            if self.fatal.is_some() {
                return;
            }
        }

        let circuit_probe = self.wait_for_circuit().await;
        if self.fatal.is_some() {
            return;
        }
        let records = std::mem::take(&mut self.rows);
        let batch_bytes = std::mem::take(&mut self.current_bytes);
        self.batch_deadline = None;
        let payload = records
            .iter()
            .map(|record| record.payload.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let request = AppendBatchRequest {
            client: self.config.client.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
            table: self.config.table.clone(),
            retry: self.config.retry,
            attempt_timeout: self.config.attempt_timeout,
        };
        lock(&self.shared).in_flight_batches += 1;
        self.in_flight.spawn(async move {
            let (result, retries) = append_batch(request, payload).await;
            BatchOutcome {
                records,
                batch_bytes,
                result,
                retries,
                circuit_probe,
            }
        });
    }

    async fn wait_for_circuit(&mut self) -> bool {
        if self.config.failure_policy != AppendFailurePolicy::Continue
            || self.config.circuit_breaker.is_none()
        {
            return false;
        }

        loop {
            let (state, remaining) = {
                let shared = lock(&self.shared);
                let remaining = shared
                    .circuit
                    .opened_until
                    .and_then(|until| until.checked_duration_since(Instant::now()));
                (shared.circuit.state, remaining)
            };
            match state {
                AppendCircuitState::Closed => return false,
                AppendCircuitState::Open => {
                    if let Some(remaining) = remaining {
                        if self.in_flight.is_empty() {
                            tokio::time::sleep(remaining).await;
                        } else {
                            tokio::select! {
                                _ = tokio::time::sleep(remaining) => {}
                                outcome = self.in_flight.join_next() => {
                                    self.handle_joined(outcome).await;
                                    if self.fatal.is_some() {
                                        return false;
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    if !self.in_flight.is_empty() {
                        self.drain_in_flight().await;
                        if self.fatal.is_some() {
                            return false;
                        }
                        continue;
                    }
                    let mut shared = lock(&self.shared);
                    if shared.circuit.state == AppendCircuitState::Open {
                        shared.circuit.state = AppendCircuitState::HalfOpen;
                        return true;
                    }
                }
                AppendCircuitState::HalfOpen => {
                    if self.in_flight.is_empty() {
                        return true;
                    }
                    let outcome = self.in_flight.join_next().await;
                    self.handle_joined(outcome).await;
                    if self.fatal.is_some() {
                        return false;
                    }
                }
            }
        }
    }

    async fn complete_barrier(&mut self, barrier: BarrierCommand) {
        self.dispatch_buffered().await;
        self.drain_in_flight().await;
        if let Some(fatal) = &self.fatal {
            let _ = barrier.ack.send(Err(fatal.to_error()));
            return;
        }
        let report = self.take_report(barrier.dropped_rows_at_barrier, barrier.started_at);
        let _ = barrier.ack.send(Ok(report));
    }

    async fn drain_in_flight(&mut self) {
        while let Some(outcome) = self.in_flight.join_next().await {
            self.handle_joined(Some(outcome)).await;
        }
    }

    async fn handle_joined(
        &mut self,
        outcome: Option<Result<BatchOutcome, tokio::task::JoinError>>,
    ) {
        let Some(outcome) = outcome else {
            return;
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let fatal = unknown_append_error(
                    "append stream background request task failed",
                    Some(error),
                );
                self.set_fatal(StreamErrorSnapshot::from_error(&fatal));
                return;
            }
        };

        let rows = outcome.records.len() as u64;
        let reserved_bytes = outcome
            .records
            .iter()
            .map(|record| record.reserved_bytes)
            .sum::<usize>();
        let mut failure_event = None;
        {
            let mut shared = lock(&self.shared);
            shared.in_flight_batches = shared.in_flight_batches.saturating_sub(1);
            shared.pending_rows = shared.pending_rows.saturating_sub(rows);
            shared.pending_bytes = shared.pending_bytes.saturating_sub(reserved_bytes);
            shared.retries = shared.retries.saturating_add(outcome.retries as u64);
            self.interval.retries = self.interval.retries.saturating_add(outcome.retries as u64);

            match &outcome.result {
                Ok(result) => {
                    let committed = result.num_rows_inserted.max(0) as u64;
                    shared.committed_rows = shared.committed_rows.saturating_add(committed);
                    self.interval.committed_rows =
                        self.interval.committed_rows.saturating_add(committed);
                    self.interval.committed_batches =
                        self.interval.committed_batches.saturating_add(1);
                    record_circuit_success(&mut shared, outcome.circuit_probe);
                }
                Err(error) => {
                    let snapshot = StreamErrorSnapshot::from_error(error);
                    let append_state = snapshot.append_state().unwrap_or(AppendState::Unknown);
                    if append_state == AppendState::Unknown {
                        shared.unknown_rows = shared.unknown_rows.saturating_add(rows);
                        self.interval.unknown_rows =
                            self.interval.unknown_rows.saturating_add(rows);
                        self.interval.unknown_batches =
                            self.interval.unknown_batches.saturating_add(1);
                    } else {
                        shared.failed_rows = shared.failed_rows.saturating_add(rows);
                        self.interval.failed_rows = self.interval.failed_rows.saturating_add(rows);
                        self.interval.failed_batches =
                            self.interval.failed_batches.saturating_add(1);
                    }
                    shared.last_failure = Some(AppendLastFailure {
                        at: SystemTime::now(),
                        message: error.message().to_string(),
                        append_state: Some(append_state),
                    });
                    let circuit_opened = self.config.failure_policy
                        == AppendFailurePolicy::Continue
                        && record_circuit_failure(
                            &mut shared,
                            self.config.circuit_breaker,
                            error,
                            outcome.circuit_probe,
                        );
                    let action = if self.config.failure_policy == AppendFailurePolicy::Stop {
                        AppendBatchFailureAction::Stopped
                    } else if circuit_opened {
                        AppendBatchFailureAction::CircuitOpened
                    } else {
                        AppendBatchFailureAction::Continuing
                    };
                    failure_event = Some(AppendBatchFailureEvent {
                        error: snapshot.to_error(),
                        batch_rows: rows as usize,
                        batch_bytes: outcome.batch_bytes,
                        outcome: append_state,
                        action,
                    });
                    if self.config.failure_policy == AppendFailurePolicy::Stop {
                        drop(shared);
                        self.set_fatal(snapshot);
                    }
                }
            }
        }
        drop(outcome.records);

        if let Some(event) = &failure_event {
            for listener in &self.config.batch_failure_listeners {
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| listener(event)));
            }
        }
    }

    fn set_fatal(&mut self, snapshot: StreamErrorSnapshot) {
        let replace = match &self.fatal {
            None => true,
            Some(current) => {
                current.append_state() != Some(AppendState::Unknown)
                    && snapshot.append_state() == Some(AppendState::Unknown)
            }
        };
        if replace {
            self.fatal = Some(snapshot.clone());
        }
        let fatal = self.fatal.clone().expect("fatal state must be set");
        let mut shared = lock(&self.shared);
        shared.lifecycle = AppendStreamState::Failed;
        shared.fatal = Some(fatal);
        drop(shared);
        self.pending_bytes.close();
    }

    fn mark_records_failed(&mut self, records: Vec<BufferedRecord>) {
        if records.is_empty() {
            return;
        }
        let rows = records.len() as u64;
        let bytes = records
            .iter()
            .map(|record| record.reserved_bytes)
            .sum::<usize>();
        let mut shared = lock(&self.shared);
        shared.failed_rows = shared.failed_rows.saturating_add(rows);
        shared.pending_rows = shared.pending_rows.saturating_sub(rows);
        shared.pending_bytes = shared.pending_bytes.saturating_sub(bytes);
        self.interval.failed_rows = self.interval.failed_rows.saturating_add(rows);
        drop(shared);
        drop(records);
    }

    async fn finish_fatal(&mut self, rx: &mut mpsc::Receiver<AppendCommand>) {
        rx.close();
        let buffered = std::mem::take(&mut self.rows);
        self.mark_records_failed(buffered);
        self.current_bytes = 0;
        self.batch_deadline = None;

        let mut barriers = Vec::new();
        while let Some(command) = rx.recv().await {
            match command {
                AppendCommand::Record(record) => {
                    self.interval.accepted_rows = self.interval.accepted_rows.saturating_add(1);
                    self.mark_records_failed(vec![record]);
                }
                AppendCommand::Flush(barrier) | AppendCommand::Shutdown(barrier) => {
                    barriers.push(barrier.ack);
                }
            }
        }
        self.drain_in_flight().await;

        let fatal = self.fatal.clone().unwrap_or_else(|| {
            StreamErrorSnapshot::from_error(&Error::new(
                ErrorKind::Unexpected,
                "append stream failed",
            ))
        });
        {
            let mut shared = lock(&self.shared);
            let resolved = shared
                .committed_rows
                .saturating_add(shared.failed_rows)
                .saturating_add(shared.unknown_rows);
            let unresolved = shared.accepted_rows.saturating_sub(resolved);
            if unresolved > 0 {
                shared.unknown_rows = shared.unknown_rows.saturating_add(unresolved);
                self.interval.unknown_rows = self.interval.unknown_rows.saturating_add(unresolved);
                self.interval.unknown_batches = self.interval.unknown_batches.saturating_add(1);
            }
            shared.pending_rows = 0;
            shared.pending_bytes = 0;
            shared.in_flight_batches = 0;
            shared.lifecycle = AppendStreamState::Failed;
            shared.fatal = Some(fatal.clone());
            shared.final_result = Some(Err(fatal.clone()));
        }
        for ack in barriers {
            let _ = ack.send(Err(fatal.to_error()));
        }
        self.pending_bytes.close();
        self.shutdown_notify.notify_waiters();
    }

    fn take_report(
        &mut self,
        dropped_rows_at_barrier: u64,
        started_at: Instant,
    ) -> AppendDeliveryReport {
        let dropped_rows = dropped_rows_at_barrier.saturating_sub(self.reported_dropped_rows);
        self.reported_dropped_rows = self.reported_dropped_rows.max(dropped_rows_at_barrier);
        let lost_rows = self
            .interval
            .failed_rows
            .saturating_add(self.interval.unknown_rows)
            .saturating_add(dropped_rows);
        let outcome = if lost_rows == 0 {
            AppendDeliveryOutcome::Ok
        } else if self.interval.committed_rows > 0 {
            AppendDeliveryOutcome::Partial
        } else if self.interval.unknown_rows > 0 {
            AppendDeliveryOutcome::Unknown
        } else {
            AppendDeliveryOutcome::Failed
        };
        let report = AppendDeliveryReport {
            outcome,
            accepted_rows: self.interval.accepted_rows,
            committed_rows: self.interval.committed_rows,
            failed_rows: self.interval.failed_rows,
            unknown_rows: self.interval.unknown_rows,
            dropped_rows,
            committed_batches: self.interval.committed_batches,
            failed_batches: self.interval.failed_batches,
            unknown_batches: self.interval.unknown_batches,
            retries: self.interval.retries,
            duration: started_at.elapsed(),
        };
        self.interval = DeliveryCounters::default();
        let mut shared = lock(&self.shared);
        shared.last_report = Some(report.clone());
        report
    }

    fn finish_closed(&mut self) {
        let report = lock(&self.shared)
            .last_report
            .clone()
            .unwrap_or_else(empty_report);
        self.finish_closed_with_report(report);
    }

    fn finish_closed_with_report(&mut self, report: AppendDeliveryReport) {
        let mut shared = lock(&self.shared);
        shared.lifecycle = AppendStreamState::Closed;
        shared.last_report = Some(report.clone());
        shared.final_result = Some(Ok(report));
        drop(shared);
        self.pending_bytes.close();
        self.shutdown_notify.notify_waiters();
    }
}

async fn append_batch(
    request: AppendBatchRequest,
    payload: String,
) -> (Result<AppendRowsResult, Error>, usize) {
    let mut retries = 0usize;
    let mut backoff = request.retry.initial_backoff;
    loop {
        let append = request.client.append_rows(
            &request.database,
            &request.schema,
            &request.table,
            payload.clone(),
        );
        let result = if let Some(timeout) = request.attempt_timeout {
            match tokio::time::timeout(timeout, append).await {
                Ok(result) => result,
                Err(error) => Err(unknown_append_error(
                    "append request timed out; commit outcome is unknown",
                    Some(error),
                )),
            }
        } else {
            append.await
        };

        match result {
            Ok(result) => return (Ok(result), retries),
            Err(error) if append_retryable(&error) && retries < request.retry.max_retries => {
                let retry_delay = retry_delay(backoff, &error, request.retry.max_backoff);
                if !retry_delay.is_zero() {
                    tokio::time::sleep(retry_delay).await;
                }
                retries += 1;
                backoff = next_backoff(backoff, request.retry.max_backoff);
            }
            Err(error) if append_retryable(&error) => {
                return (
                    Err(error.with_context("retries", retries).set_persistent()),
                    retries,
                );
            }
            Err(error) => return (Err(error), retries),
        }
    }
}

fn append_retryable(error: &Error) -> bool {
    error.is_temporary()
        && error
            .append_details()
            .is_some_and(|details| details.append_state == AppendState::Rejected)
}

fn unknown_append_error<E>(message: impl Into<String>, source: Option<E>) -> Error
where
    E: Into<anyhow::Error>,
{
    let mut error =
        Error::new(ErrorKind::AppendRowsFailed, message).set_append_details(AppendErrorDetails {
            append_state: AppendState::Unknown,
            row_errors: Vec::new(),
            row_errors_truncated: false,
        });
    if let Some(source) = source {
        error = error.set_source(source);
    }
    error.set_persistent()
}

fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }
    current
        .checked_mul(2)
        .unwrap_or(max_backoff)
        .min(max_backoff)
}

fn retry_delay(backoff: Duration, error: &Error, max_backoff: Duration) -> Duration {
    backoff
        .max(error.retry_after().unwrap_or(Duration::ZERO))
        .min(max_backoff)
}

fn try_claim_circuit_admission(shared: &mut SharedState) -> bool {
    match shared.circuit.state {
        AppendCircuitState::Closed => true,
        AppendCircuitState::HalfOpen => false,
        AppendCircuitState::Open => {
            let cooled_down = shared
                .circuit
                .opened_until
                .is_none_or(|until| Instant::now() >= until);
            if cooled_down && !shared.circuit.probe_admission_claimed {
                shared.circuit.probe_admission_claimed = true;
                true
            } else {
                false
            }
        }
    }
}

fn circuit_admission_available(shared: &SharedState) -> bool {
    match shared.circuit.state {
        AppendCircuitState::Closed => true,
        AppendCircuitState::HalfOpen => false,
        AppendCircuitState::Open => {
            shared
                .circuit
                .opened_until
                .is_none_or(|until| Instant::now() >= until)
                && !shared.circuit.probe_admission_claimed
        }
    }
}

fn record_circuit_success(shared: &mut SharedState, probe: bool) {
    shared.circuit.consecutive_failures = 0;
    if probe {
        shared.circuit.state = AppendCircuitState::Closed;
        shared.circuit.opened_until = None;
        shared.circuit.probe_admission_claimed = false;
    }
}

fn record_circuit_failure(
    shared: &mut SharedState,
    options: Option<AppendCircuitBreakerOptions>,
    error: &Error,
    probe: bool,
) -> bool {
    let Some(options) = options else {
        return false;
    };
    let availability_failure = error.is_temporary()
        || error.is_persistent()
        || error
            .append_details()
            .is_some_and(|details| details.append_state == AppendState::Unknown);
    if !availability_failure {
        if probe {
            record_circuit_success(shared, true);
        }
        return false;
    }

    shared.circuit.consecutive_failures = shared.circuit.consecutive_failures.saturating_add(1);
    if probe || shared.circuit.consecutive_failures >= options.failure_threshold {
        shared.circuit.state = AppendCircuitState::Open;
        shared.circuit.opened_until = Some(deadline_after(options.cooldown));
        shared.circuit.probe_admission_claimed = false;
        true
    } else {
        false
    }
}

#[derive(Debug, Clone, Copy)]
enum DropReason {
    BufferFull,
    InvalidRecord,
    RecordTooLarge,
    CircuitOpen,
    Closed,
}

fn note_drop(shared: &mut SharedState, reason: DropReason) {
    shared.dropped_rows = shared.dropped_rows.saturating_add(1);
    let counter = match reason {
        DropReason::BufferFull => &mut shared.dropped_by_reason.buffer_full,
        DropReason::InvalidRecord => &mut shared.dropped_by_reason.invalid_record,
        DropReason::RecordTooLarge => &mut shared.dropped_by_reason.record_too_large,
        DropReason::CircuitOpen => &mut shared.dropped_by_reason.circuit_open,
        DropReason::Closed => &mut shared.dropped_by_reason.closed,
    };
    *counter = counter.saturating_add(1);
}

fn empty_report() -> AppendDeliveryReport {
    AppendDeliveryReport {
        outcome: AppendDeliveryOutcome::Ok,
        accepted_rows: 0,
        committed_rows: 0,
        failed_rows: 0,
        unknown_rows: 0,
        dropped_rows: 0,
        committed_batches: 0,
        failed_batches: 0,
        unknown_batches: 0,
        retries: 0,
        duration: Duration::ZERO,
    }
}

fn deadline_after(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration).unwrap_or(now)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    use super::*;

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        target: String,
        headers: HashMap<String, String>,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct MockResponse {
        status: u16,
        body: String,
        delay: Duration,
    }

    impl MockResponse {
        fn json(status: u16, body: impl Into<String>) -> Self {
            Self {
                status,
                body: body.into(),
                delay: Duration::ZERO,
            }
        }

        fn committed(request: &RecordedRequest) -> Self {
            let rows = request
                .body
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count();
            Self::json(
                200,
                format!(r#"{{"append_state":"committed","num_rows_inserted":{rows}}}"#),
            )
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    type Responder = dyn Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync;

    struct MockServer {
        endpoint: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        max_active: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
        accept_task: Option<thread::JoinHandle<()>>,
    }

    impl MockServer {
        fn start<F>(responder: F) -> Self
        where
            F: Fn(usize, &RecordedRequest) -> MockResponse + Send + Sync + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = format!("http://{address}");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let active = Arc::new(AtomicUsize::new(0));
            let max_active = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let responder: Arc<Responder> = Arc::new(responder);

            let accept_task = thread::spawn({
                let requests = requests.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                let stop = stop.clone();
                move || {
                    while !stop.load(Ordering::Acquire) {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                if stop.load(Ordering::Acquire) {
                                    break;
                                }
                                let requests = requests.clone();
                                let active = active.clone();
                                let max_active = max_active.clone();
                                let responder = responder.clone();
                                thread::spawn(move || {
                                    handle_connection(
                                        stream, requests, active, max_active, responder,
                                    );
                                });
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(1));
                            }
                            Err(_) => break,
                        }
                    }
                }
            });

            Self {
                endpoint,
                requests,
                max_active,
                stop,
                accept_task: Some(accept_task),
            }
        }

        fn client(&self) -> Client {
            let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
            Client::new(&self.endpoint, http_client).unwrap()
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            lock(&self.requests).clone()
        }

        fn max_active(&self) -> usize {
            self.max_active.load(Ordering::Acquire)
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(task) = self.accept_task.take() {
                let _ = task.join();
            }
        }
    }

    fn handle_connection(
        mut stream: TcpStream,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        responder: Arc<Responder>,
    ) {
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let Some(request) = read_request(&mut stream) else {
            return;
        };
        let index = {
            let mut recorded = lock(&requests);
            let index = recorded.len();
            recorded.push(request.clone());
            index
        };
        let now_active = active.fetch_add(1, Ordering::AcqRel) + 1;
        max_active.fetch_max(now_active, Ordering::AcqRel);
        let response = responder(index, &request);
        if !response.delay.is_zero() {
            thread::sleep(response.delay);
        }
        let reason = match response.status {
            200 => "OK",
            400 => "Bad Request",
            422 => "Unprocessable Entity",
            503 => "Service Unavailable",
            _ => "Test Response",
        };
        let wire = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.status,
            reason,
            response.body.len(),
            response.body
        );
        let _ = stream.write_all(wire.as_bytes());
        let _ = stream.flush();
        active.fetch_sub(1, Ordering::AcqRel);
    }

    fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers_text = String::from_utf8(bytes[..header_end].to_vec()).ok()?;
        let mut lines = headers_text.split("\r\n");
        let target = lines.next()?.split_whitespace().nth(1)?.to_string();
        let mut headers = HashMap::new();
        for line in lines.filter(|line| !line.is_empty()) {
            let (name, value) = line.split_once(':')?;
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len().saturating_sub(header_end) < content_length {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        let body = String::from_utf8(
            bytes[header_end..header_end.saturating_add(content_length)].to_vec(),
        )
        .ok()?;
        Some(RecordedRequest {
            target,
            headers,
            body,
        })
    }

    #[test]
    fn mock_server_waits_for_delayed_request_bytes() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let mut stream = TcpStream::connect(server.endpoint.trim_start_matches("http://")).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        thread::sleep(Duration::from_millis(20));
        let body = "{\"id\":1}\n";
        let request = format!(
            "POST /rows HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert_eq!(server.requests().len(), 1);
    }

    #[test]
    fn rejects_non_object_rows() {
        assert!(matches!(
            prepare_record_for_try_send(&vec![1, 2, 3]),
            Err(AppendTrySendError::InvalidRecord)
        ));
    }

    #[test]
    fn report_outcomes_include_drops_and_unknown_rows() {
        let config = AppendStreamConfig {
            client: Client::new("http://127.0.0.1:1", reqwest::Client::new()).unwrap(),
            database: "scopedb".to_string(),
            schema: "public".to_string(),
            table: "events".to_string(),
            batch_bytes: 1024,
            max_batch_rows: 100,
            flush_interval: Duration::from_secs(1),
            channel_capacity: 1,
            max_pending_bytes: 1024,
            max_in_flight_requests: 1,
            retry: RetryConfig {
                max_retries: 0,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            failure_policy: AppendFailurePolicy::Continue,
            attempt_timeout: Some(Duration::from_secs(1)),
            circuit_breaker: None,
            batch_failure_listeners: Vec::new(),
        };
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let budget = Arc::new(PendingBytesBudget::new(1024));
        let mut worker = AppendWorker {
            config,
            shared,
            pending_bytes: budget,
            shutdown_notify: Arc::new(Notify::new()),
            rows: Vec::new(),
            current_bytes: 0,
            batch_deadline: None,
            in_flight: JoinSet::new(),
            interval: DeliveryCounters {
                accepted_rows: 2,
                committed_rows: 1,
                unknown_rows: 1,
                ..DeliveryCounters::default()
            },
            reported_dropped_rows: 0,
            fatal: None,
        };
        let report = worker.take_report(1, Instant::now());
        assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
        assert_eq!(report.dropped_rows, 1);
    }

    #[test]
    fn backoff_is_capped() {
        assert_eq!(
            next_backoff(Duration::from_secs(4), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn retry_after_is_honored_and_capped() {
        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_retry_after(Duration::from_secs(30));
        assert_eq!(
            retry_delay(Duration::from_millis(100), &error, Duration::from_secs(5)),
            Duration::from_secs(5)
        );

        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_retry_after(Duration::from_millis(200));
        assert_eq!(
            retry_delay(Duration::from_secs(1), &error, Duration::from_secs(5)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn max_batch_rows_cannot_exceed_protocol_limit() {
        let result = Client::new("https://example.com", reqwest::Client::new())
            .unwrap()
            .table("events")
            .append_stream()
            .max_batch_rows(MAX_APPEND_ROWS + 1)
            .build();
        let error = match result {
            Ok(_) => panic!("invalid max_batch_rows was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn error_snapshot_preserves_http_metadata() {
        let error = Error::new(ErrorKind::AppendRowsFailed, "busy")
            .set_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            .set_request_id("request-123".to_string())
            .set_retry_after(Duration::from_secs(2))
            .set_temporary();
        let restored = StreamErrorSnapshot::from_error(&error).to_error();
        assert_eq!(
            restored.http_status(),
            Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(restored.request_id(), Some("request-123"));
        assert_eq!(restored.retry_after(), Some(Duration::from_secs(2)));
        assert!(restored.is_temporary());
    }

    #[tokio::test]
    async fn batches_ndjson_and_runs_requests_concurrently() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(30))
        });
        let stream = server
            .client()
            .table("events/archive")
            .with_database("analytics 2026")
            .with_schema("public")
            .append_stream()
            .target_batch_bytes(1)
            .max_concurrent_batches(3)
            .build()
            .unwrap();

        let admitted = stream
            .send_all((0..6).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(admitted.accepted_rows, 6);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Ok);
        assert_eq!(report.accepted_rows, 6);
        assert_eq!(report.committed_rows, 6);
        assert_eq!(report.committed_batches, 6);
        assert!(server.max_active() >= 2);
        let requests = server.requests();
        assert_eq!(requests.len(), 6);
        for request in requests {
            assert_eq!(
                request.target,
                "/v1/databases/analytics%202026/schemas/public/tables/events%2Farchive/rows"
            );
            assert_eq!(
                request.headers.get("content-type").map(String::as_str),
                Some("application/x-ndjson")
            );
            assert_eq!(request.body.lines().count(), 1);
            assert!(request.body.starts_with('{'));
        }
    }

    #[tokio::test]
    async fn semantic_builder_names_split_batches_by_row_count() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .target_batch_bytes(1024 * 1024)
            .max_batch_rows(2)
            .max_buffered_bytes(1024 * 1024)
            .max_concurrent_batches(1)
            .build()
            .unwrap();

        stream
            .send_all((0..5).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(report.committed_rows, 5);
        let row_counts = server
            .requests()
            .iter()
            .map(|request| request.body.lines().count())
            .collect::<Vec<_>>();
        assert_eq!(row_counts, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn client_builder_attaches_api_key_to_requests() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
        let client = Client::builder(&server.endpoint)
            .api_key("secret-api-key")
            .http_client(http_client)
            .build()
            .unwrap();

        client.table("events").append(r#"{"id":1}"#).await.unwrap();

        assert_eq!(
            server.requests()[0]
                .headers
                .get("authorization")
                .map(String::as_str),
            Some("Bearer secret-api-key")
        );
    }

    #[tokio::test]
    async fn continue_mode_retries_only_temporary_rejections() {
        let server = MockServer::start(|index, request| match index {
            0 => MockResponse::json(503, r#"{"message":"busy","append_state":"rejected"}"#),
            1 => MockResponse::committed(request),
            2 => MockResponse::json(503, r#"{"message":"ambiguous","append_state":"unknown"}"#),
            3 => MockResponse::json(422, r#"{"message":"bad row","append_state":"rejected"}"#),
            _ => panic!("unexpected retry request {index}"),
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .failure_policy(AppendFailurePolicy::Continue)
            .circuit_breaker(None)
            .target_batch_bytes(1)
            .max_concurrent_batches(1)
            .max_retries(4)
            .initial_backoff(Duration::ZERO)
            .max_backoff(Duration::ZERO)
            .build()
            .unwrap();

        stream
            .send_all((0..3).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(server.requests().len(), 4);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Partial);
        assert_eq!(report.accepted_rows, 3);
        assert_eq!(report.committed_rows, 1);
        assert_eq!(report.failed_rows, 1);
        assert_eq!(report.unknown_rows, 1);
        assert_eq!(report.committed_batches, 1);
        assert_eq!(report.failed_batches, 1);
        assert_eq!(report.unknown_batches, 1);
        assert_eq!(report.retries, 1);
        assert_eq!(
            report.accepted_rows,
            report.committed_rows + report.failed_rows + report.unknown_rows
        );
    }

    #[tokio::test]
    async fn unknown_timeout_is_not_retried() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(100))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .failure_policy(AppendFailurePolicy::Continue)
            .circuit_breaker(None)
            .target_batch_bytes(1)
            .max_retries(8)
            .attempt_timeout(Duration::from_millis(10))
            .build()
            .unwrap();

        stream.send(&serde_json::json!({"id": 1})).await.unwrap();
        let report = stream.shutdown().await.unwrap();

        assert_eq!(server.requests().len(), 1);
        assert_eq!(report.outcome, AppendDeliveryOutcome::Unknown);
        assert_eq!(report.unknown_rows, 1);
        assert_eq!(report.retries, 0);
    }

    #[tokio::test]
    async fn stop_policy_closes_after_a_rejected_batch() {
        let server = MockServer::start(|_, _| {
            MockResponse::json(422, r#"{"message":"invalid","append_state":"rejected"}"#)
                .with_delay(Duration::from_millis(20))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .target_batch_bytes(1)
            .max_concurrent_batches(1)
            .build()
            .unwrap();
        stream
            .send_all((0..2).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();

        let error = stream.shutdown().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::AppendRowsFailed);
        assert_eq!(
            error.append_details().map(|details| details.append_state),
            Some(AppendState::Rejected)
        );
        let stats = stream.stats();
        assert_eq!(stats.state, AppendStreamState::Failed);
        assert_eq!(stats.accepted_rows, 2);
        assert_eq!(stats.failed_rows, 2);
        assert_eq!(stats.pending_rows, 0);
        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 3})),
            Err(AppendTrySendError::Closed)
        ));
    }

    #[tokio::test]
    async fn strict_terminal_waits_for_every_in_flight_outcome() {
        let server = MockServer::start(|index, _| match index {
            0 => MockResponse::json(422, r#"{"message":"invalid","append_state":"rejected"}"#)
                .with_delay(Duration::from_millis(5)),
            1 => MockResponse::json(503, r#"{"message":"ambiguous","append_state":"unknown"}"#)
                .with_delay(Duration::from_millis(80)),
            _ => panic!("unexpected request {index}"),
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .target_batch_bytes(1)
            .max_concurrent_batches(2)
            .build()
            .unwrap();
        stream
            .send_all((0..2).map(|id| serde_json::json!({"id": id})))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while stream.stats().state != AppendStreamState::Failed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let started = Instant::now();
        let error = stream.shutdown().await.unwrap_err();

        assert!(started.elapsed() >= Duration::from_millis(40));
        assert_eq!(
            error.append_details().map(|details| details.append_state),
            Some(AppendState::Unknown)
        );
        let stats = stream.stats();
        assert_eq!(stats.failed_rows, 1);
        assert_eq!(stats.unknown_rows, 1);
        assert_eq!(stats.pending_rows, 0);
    }

    #[tokio::test]
    async fn concurrent_flush_and_shutdown_both_settle() {
        let server = MockServer::start(|_, request| {
            MockResponse::committed(request).with_delay(Duration::from_millis(5))
        });
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .build()
            .unwrap();
        stream.send(&serde_json::json!({"id": 1})).await.unwrap();

        let (flush, shutdown) = tokio::join!(stream.flush(), stream.shutdown());
        assert!(flush.is_ok(), "flush failed: {flush:?}");
        assert!(shutdown.is_ok(), "shutdown failed: {shutdown:?}");
        assert_eq!(stream.stats().state, AppendStreamState::Closed);
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_across_clones() {
        let server = MockServer::start(|_, request| MockResponse::committed(request));
        let stream = server
            .client()
            .table("events")
            .append_stream()
            .build()
            .unwrap();
        let clone = stream.clone();
        stream.send(&serde_json::json!({"id": 1})).await.unwrap();

        let (first, second) = tokio::join!(stream.shutdown(), clone.shutdown());
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(server.requests().len(), 1);
        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 2})),
            Err(AppendTrySendError::Closed)
        ));
    }

    #[tokio::test]
    async fn try_send_accounts_for_local_drops() {
        let client = Client::new("http://127.0.0.1:1", reqwest::Client::new()).unwrap();
        let stream = client
            .table("events")
            .append_stream()
            .failure_policy(AppendFailurePolicy::Continue)
            .max_buffered_bytes(1)
            .build()
            .unwrap();

        assert!(matches!(
            stream.try_send(&serde_json::json!({"id": 1})),
            Err(AppendTrySendError::BufferFull)
        ));
        let report = stream.shutdown().await.unwrap();
        assert_eq!(report.outcome, AppendDeliveryOutcome::Failed);
        assert_eq!(report.accepted_rows, 0);
        assert_eq!(report.dropped_rows, 1);
        assert_eq!(stream.stats().dropped_by_reason.buffer_full, 1);
    }
}
