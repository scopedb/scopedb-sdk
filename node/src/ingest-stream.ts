/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { Client } from "./client.js";
import { ScopeDBError } from "./errors.js";
import type { IngestResult } from "./protocol.js";

const DEFAULT_BATCH_BYTES = 16 * 1024 * 1024;
const DEFAULT_FLUSH_INTERVAL_MS = 1_000;
const DEFAULT_CHANNEL_CAPACITY = 1024;
const DEFAULT_MAX_PENDING_BYTES = DEFAULT_BATCH_BYTES * 4;
const DEFAULT_MAX_RETRIES = 8;
const DEFAULT_INITIAL_BACKOFF_MS = 100;
const DEFAULT_MAX_BACKOFF_MS = 5_000;

const TEXT_ENCODER = new TextEncoder();

type FatalStatus = "permanent" | "temporary" | "persistent";

interface FatalState {
  message: string;
  status: FatalStatus;
}

interface RetryConfig {
  maxRetries: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
}

interface BufferedRecord {
  payload: string;
  bytes: number;
  reservation: PendingBytesReservation;
}

interface FlushCommand {
  type: "flush";
  ack: Deferred<IngestResult | null>;
}

interface ShutdownCommand {
  type: "shutdown";
  ack: Deferred<IngestResult | null>;
}

interface RecordCommand {
  type: "record";
  record: BufferedRecord;
}

type BatchCommand = RecordCommand | FlushCommand | ShutdownCommand;

export class IngestStreamBuilder {
  private currentBatchBytes = DEFAULT_BATCH_BYTES;
  private currentFlushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
  private currentChannelCapacity = DEFAULT_CHANNEL_CAPACITY;
  private currentMaxPendingBytes = DEFAULT_MAX_PENDING_BYTES;
  private currentRetry: RetryConfig = {
    maxRetries: DEFAULT_MAX_RETRIES,
    initialBackoffMs: DEFAULT_INITIAL_BACKOFF_MS,
    maxBackoffMs: DEFAULT_MAX_BACKOFF_MS,
  };

  constructor(
    private readonly client: Client,
    private readonly statement: string,
  ) {}

  batchBytes(batchBytes: number): this {
    this.currentBatchBytes = Math.max(1, batchBytes);
    return this;
  }

  flushInterval(flushIntervalMs: number): this {
    this.currentFlushIntervalMs = Math.max(1, flushIntervalMs);
    return this;
  }

  channelCapacity(channelCapacity: number): this {
    this.currentChannelCapacity = Math.max(1, channelCapacity);
    return this;
  }

  maxPendingBytes(maxPendingBytes: number): this {
    this.currentMaxPendingBytes = Math.max(1, maxPendingBytes);
    return this;
  }

  maxRetries(maxRetries: number): this {
    this.currentRetry.maxRetries = maxRetries;
    return this;
  }

  initialBackoff(initialBackoffMs: number): this {
    this.currentRetry.initialBackoffMs = Math.max(0, initialBackoffMs);
    return this;
  }

  maxBackoff(maxBackoffMs: number): this {
    this.currentRetry.maxBackoffMs = Math.max(0, maxBackoffMs);
    return this;
  }

  build(): IngestStream {
    return new IngestStream(
      this.client,
      this.statement,
      this.currentBatchBytes,
      this.currentFlushIntervalMs,
      this.currentChannelCapacity,
      this.currentMaxPendingBytes,
      { ...this.currentRetry },
    );
  }
}

export class IngestStream {
  private fatal: FatalState | null = null;
  private readonly queue: AsyncBoundedQueue<BatchCommand>;
  private readonly pendingBytes: PendingBytesBudget;
  private readonly task: Promise<void>;

  constructor(
    private readonly client: Client,
    private readonly statement: string,
    private readonly batchBytes: number,
    private readonly flushIntervalMs: number,
    channelCapacity: number,
    maxPendingBytes: number,
    private readonly retry: RetryConfig,
  ) {
    this.queue = new AsyncBoundedQueue(channelCapacity);
    this.pendingBytes = new PendingBytesBudget(maxPendingBytes);
    this.task = this.runWorker();
  }

  /** Enqueues a single record for batched ingestion. Blocks when backpressure is applied. */
  async send(record: unknown): Promise<void> {
    this.checkFatal();
    const payload = serializeRecord(record);
    const bytes = bufferedBytes(payload);

    let reservation: PendingBytesReservation;
    try {
      reservation = await this.pendingBytes.acquire(bytes);
    } catch (cause) {
      throw this.mapPendingBytesError(cause, bytes);
    }

    try {
      await this.queue.send({
        type: "record",
        record: { payload, bytes: byteLength(payload), reservation },
      });
    } catch {
      reservation.release();
      throw this.closedOrFatalError();
    }

    this.checkFatal();
  }

  /**
   * Flushes any buffered records to the server immediately, without closing the stream.
   *
   * This is an optional checkpoint operation. You do NOT need to call `flush()`
   * before `shutdown()` — `shutdown()` already flushes remaining records before closing.
   *
   * Returns the ingest result for the flushed batch, or `null` if there was nothing to flush.
   */
  async flush(): Promise<IngestResult | null> {
    this.checkFatal();
    const ack = new Deferred<IngestResult | null>();
    try {
      await this.queue.send({ type: "flush", ack });
    } catch {
      throw this.closedOrFatalError();
    }
    return waitForAck(ack, () => this.closedOrFatalError());
  }

  /**
   * Flushes any remaining buffered records and shuts down the stream.
   *
   * Always call `shutdown()` when you are done sending records. It automatically
   * flushes whatever is still in the buffer — you do not need to call `flush()` first.
   *
   * Returns the ingest result for the final batch, or `null` if there were no pending records.
   */
  async shutdown(): Promise<IngestResult | null> {
    const ack = new Deferred<IngestResult | null>();
    try {
      await this.queue.send({ type: "shutdown", ack });
    } catch {
      throw this.closedOrFatalError();
    }
    const result = await waitForAck(ack, () => this.closedOrFatalError());
    await this.task;
    return result;
  }

  private async runWorker(): Promise<void> {
    const rows: BufferedRecord[] = [];
    let currentBytes = 0;

    try {
      for (;;) {
        const command = await this.queue.receive(this.flushIntervalMs);
        if (command === QUEUE_TIMEOUT) {
          if (rows.length === 0) {
            continue;
          }
          try {
            await this.flushPending(rows, () => {
              currentBytes = 0;
            });
          } catch (cause) {
            this.setFatal(asFatalState(cause));
            break;
          }
          continue;
        }

        if (command === QUEUE_CLOSED) {
          try {
            await this.flushPending(rows, () => {
              currentBytes = 0;
            });
          } catch (cause) {
            this.setFatal(asFatalState(cause));
          }
          break;
        }

        switch (command.type) {
          case "record":
            if (rows.length > 0) {
              currentBytes += 1;
            }
            currentBytes += command.record.bytes;
            rows.push(command.record);

            if (currentBytes >= this.batchBytes) {
              try {
                await this.flushPending(rows, () => {
                  currentBytes = 0;
                });
              } catch (cause) {
                this.setFatal(asFatalState(cause));
                return;
              }
            }
            break;
          case "flush":
            try {
              command.ack.resolve(await this.flushPending(rows, () => {
                currentBytes = 0;
              }));
            } catch (cause) {
              const error = asScopeDBError(cause);
              this.setFatal(asFatalState(error));
              command.ack.reject(error);
              return;
            }
            break;
          case "shutdown":
            try {
              command.ack.resolve(await this.flushPending(rows, () => {
                currentBytes = 0;
              }));
            } catch (cause) {
              const error = asScopeDBError(cause);
              this.setFatal(asFatalState(error));
              command.ack.reject(error);
            }
            return;
        }
      }
    } finally {
      releaseRows(rows);
      this.queue.close();
      this.pendingBytes.close();
    }
  }

  private async flushPending(
    rows: BufferedRecord[],
    onSuccess: () => void,
  ): Promise<IngestResult | null> {
    if (rows.length === 0) {
      return null;
    }

    const payload = rows.map((row) => row.payload).join("\n");
    let retries = 0;
    let backoffMs = this.retry.initialBackoffMs;

    for (;;) {
      try {
        const result = await this.client.insert(payload, this.statement);
        releaseRows(rows);
        rows.length = 0;
        onSuccess();
        return result;
      } catch (cause) {
        const error = asScopeDBError(cause);
        if (error.isTemporary() && retries < this.retry.maxRetries) {
          retries += 1;
          if (backoffMs > 0) {
            await sleep(backoffMs);
          }
          backoffMs = nextBackoff(backoffMs, this.retry.maxBackoffMs);
          continue;
        }
        if (error.isTemporary()) {
          throw retryExhaustedError(retries, error);
        }
        throw error;
      }
    }
  }

  private setFatal(fatal: FatalState): void {
    if (this.fatal === null) {
      this.fatal = fatal;
    }
  }

  private checkFatal(): void {
    if (this.fatal !== null) {
      throw fatalToError(this.fatal);
    }
  }

  private closedOrFatalError(): ScopeDBError {
    if (this.fatal !== null) {
      return fatalToError(this.fatal);
    }
    return new ScopeDBError("Unexpected", "ingest stream is closed").setPersistent();
  }

  private mapPendingBytesError(cause: unknown, requested: number): ScopeDBError {
    if (cause instanceof PendingBytesClosedError) {
      return this.closedOrFatalError();
    }
    if (cause instanceof PendingBytesExceedsCapacityError) {
      return new ScopeDBError(
        "Unexpected",
        `ingest stream record requires ${requested} buffered bytes, exceeds max_pending_bytes=${cause.capacity}`,
      ).setPermanent();
    }
    return asScopeDBError(cause);
  }
}

class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;
  reject!: (reason?: unknown) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

class QueueClosedError extends Error {}

const QUEUE_TIMEOUT = Symbol("QUEUE_TIMEOUT");
const QUEUE_CLOSED = Symbol("QUEUE_CLOSED");

type QueueReceiveResult<T> = T | typeof QUEUE_TIMEOUT | typeof QUEUE_CLOSED;

class AsyncBoundedQueue<T> {
  private readonly items: T[] = [];
  private readonly sendWaiters: Array<{ item: T; ack: Deferred<void> }> = [];
  private readonly recvWaiters: Array<{
    deferred: Deferred<QueueReceiveResult<T>>;
    timer?: ReturnType<typeof setTimeout>;
  }> = [];
  private closed = false;

  constructor(private readonly capacity: number) {}

  async send(item: T): Promise<void> {
    if (this.closed) {
      throw new QueueClosedError();
    }

    const recvWaiter = this.recvWaiters.shift();
    if (recvWaiter !== undefined) {
      if (recvWaiter.timer !== undefined) {
        clearTimeout(recvWaiter.timer);
      }
      recvWaiter.deferred.resolve(item);
      return;
    }

    if (this.items.length < this.capacity) {
      this.items.push(item);
      return;
    }

    const ack = new Deferred<void>();
    this.sendWaiters.push({ item, ack });
    await ack.promise;
  }

  async receive(timeoutMs: number): Promise<QueueReceiveResult<T>> {
    if (this.items.length > 0) {
      const item = this.items.shift()!;
      this.drainSenders();
      return item;
    }

    if (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters.shift()!;
      waiter.ack.resolve();
      return waiter.item;
    }

    if (this.closed) {
      return QUEUE_CLOSED;
    }

    const deferred = new Deferred<QueueReceiveResult<T>>();
    const waiter: {
      deferred: Deferred<QueueReceiveResult<T>>;
      timer?: ReturnType<typeof setTimeout>;
    } = { deferred };
    if (timeoutMs > 0) {
      waiter.timer = setTimeout(() => {
        const index = this.recvWaiters.indexOf(waiter);
        if (index >= 0) {
          this.recvWaiters.splice(index, 1);
        }
        deferred.resolve(QUEUE_TIMEOUT);
      }, timeoutMs);
    }
    this.recvWaiters.push(waiter);
    return deferred.promise;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    while (this.sendWaiters.length > 0) {
      this.sendWaiters.shift()!.ack.reject(new QueueClosedError());
    }
    while (this.recvWaiters.length > 0) {
      const waiter = this.recvWaiters.shift()!;
      if (waiter.timer !== undefined) {
        clearTimeout(waiter.timer);
      }
      waiter.deferred.resolve(QUEUE_CLOSED);
    }
  }

  private drainSenders(): void {
    while (this.sendWaiters.length > 0) {
      const waiter = this.sendWaiters[0]!;
      const recvWaiter = this.recvWaiters.shift();
      if (recvWaiter !== undefined) {
        this.sendWaiters.shift();
        if (recvWaiter.timer !== undefined) {
          clearTimeout(recvWaiter.timer);
        }
        recvWaiter.deferred.resolve(waiter.item);
        waiter.ack.resolve();
        continue;
      }
      if (this.items.length >= this.capacity) {
        break;
      }
      this.sendWaiters.shift();
      this.items.push(waiter.item);
      waiter.ack.resolve();
    }
  }
}

class PendingBytesClosedError extends Error {}

class PendingBytesExceedsCapacityError extends Error {
  constructor(readonly capacity: number) {
    super("pending bytes request exceeds capacity");
  }
}

class PendingBytesReservation {
  private released = false;

  constructor(
    private readonly budget: PendingBytesBudget,
    readonly permits: number,
  ) {}

  release(): void {
    if (this.released) {
      return;
    }
    this.released = true;
    this.budget.release(this.permits);
  }
}

class PendingBytesBudget {
  private available: number;
  private readonly waiters: Array<{
    requested: number;
    deferred: Deferred<PendingBytesReservation>;
  }> = [];
  private closed = false;

  constructor(private readonly capacity: number) {
    this.available = capacity;
  }

  async acquire(requested: number): Promise<PendingBytesReservation> {
    if (requested > this.capacity) {
      throw new PendingBytesExceedsCapacityError(this.capacity);
    }
    if (this.closed) {
      throw new PendingBytesClosedError();
    }
    if (requested <= this.available) {
      this.available -= requested;
      return new PendingBytesReservation(this, requested);
    }

    const deferred = new Deferred<PendingBytesReservation>();
    this.waiters.push({ requested, deferred });
    return deferred.promise;
  }

  release(permits: number): void {
    this.available += permits;
    this.drainWaiters();
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    while (this.waiters.length > 0) {
      this.waiters.shift()!.deferred.reject(new PendingBytesClosedError());
    }
  }

  private drainWaiters(): void {
    while (this.waiters.length > 0) {
      if (this.closed) {
        this.close();
        return;
      }
      const next = this.waiters[0]!;
      if (next.requested > this.available) {
        return;
      }
      this.waiters.shift();
      this.available -= next.requested;
      next.deferred.resolve(new PendingBytesReservation(this, next.requested));
    }
  }
}

function serializeRecord(record: unknown): string {
  try {
    const payload = JSON.stringify(record);
    if (payload === undefined) {
      throw new ScopeDBError(
        "Unexpected",
        "failed to serialize batched ingest record: record produced undefined JSON",
      );
    }
    return payload;
  } catch (cause) {
    if (cause instanceof ScopeDBError) {
      throw cause;
    }
    throw new ScopeDBError("Unexpected", "failed to serialize batched ingest record", {
      cause,
    });
  }
}

function bufferedBytes(payload: string): number {
  return byteLength(payload) + 1;
}

function byteLength(payload: string): number {
  return TEXT_ENCODER.encode(payload).byteLength;
}

function releaseRows(rows: BufferedRecord[]): void {
  for (const row of rows) {
    row.reservation.release();
  }
}

function nextBackoff(currentMs: number, maxBackoffMs: number): number {
  if (currentMs === 0) {
    return 0;
  }
  return Math.min(currentMs * 2, maxBackoffMs);
}

function retryExhaustedError(retries: number, cause: ScopeDBError): ScopeDBError {
  return new ScopeDBError("Unexpected", "ingest stream flush exhausted retry budget", {
    cause,
  })
    .withContext("retries", retries)
    .withContext("last_error", cause.message)
    .setPersistent();
}

function asFatalState(cause: unknown): FatalState {
  const error = asScopeDBError(cause);
  let status: FatalStatus = "permanent";
  if (error.isTemporary()) {
    status = "temporary";
  } else if (error.isPersistent()) {
    status = "persistent";
  }
  return {
    message: error.message,
    status,
  };
}

function fatalToError(fatal: FatalState): ScopeDBError {
  const error = new ScopeDBError("Unexpected", fatal.message);
  switch (fatal.status) {
    case "temporary":
      return error.setTemporary();
    case "persistent":
      return error.setPersistent();
    case "permanent":
      return error.setPermanent();
  }
}

function asScopeDBError(cause: unknown): ScopeDBError {
  if (cause instanceof ScopeDBError) {
    return cause;
  }
  if (cause instanceof Error) {
    return new ScopeDBError("Unexpected", cause.message, { cause });
  }
  return new ScopeDBError("Unexpected", String(cause));
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitForAck<T>(
  deferred: Deferred<T>,
  makeClosedError: () => ScopeDBError,
): Promise<T> {
  try {
    return await deferred.promise;
  } catch (cause) {
    if (cause instanceof ScopeDBError) {
      throw cause;
    }
    throw makeClosedError();
  }
}
