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

mod common;

use std::time::Duration;

use scopedb_client::AppendDeliveryOutcome;
use scopedb_client::AppendFailurePolicy;
use scopedb_client::AppendStream;
use uuid::Uuid;

fn track(stream: &AppendStream, request: u64) -> bool {
    match stream.try_send(&serde_json::json!({
        "event_id": Uuid::now_v7(),
        "occurred_at": jiff::Timestamp::now(),
        "name": "request.completed",
        "attributes": {"request": request, "status": 200},
    })) {
        // This means local admission only, not a remote commit.
        Ok(()) => true,
        Err(error) => {
            // Route this diagnostic to another sink, never back into `stream`.
            eprintln!("telemetry row dropped locally: {error}");
            false
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let table = common::write_table(&client)?;

    // Continue mode is explicitly best effort: failed or ambiguous batches are
    // accounted for, released, and do not stop later telemetry batches.
    let telemetry = table
        .append_stream()
        .failure_policy(AppendFailurePolicy::Continue)
        .target_batch_bytes(1024 * 1024)
        .max_batch_rows(10_000)
        .flush_interval(Duration::from_secs(1))
        .max_buffered_bytes(32 * 1024 * 1024)
        .max_concurrent_batches(2)
        .attempt_timeout(Duration::from_secs(10))
        .on_batch_failure(|event| {
            // A synchronous observer; use a separate diagnostics sink.
            eprintln!("telemetry append failure: {event:?}");
        })
        .build()?;

    let mut dropped_locally = 0_u64;
    for request in 1..=100 {
        if !track(&telemetry, request) {
            dropped_locally += 1;
        }
    }
    println!("locally dropped: {dropped_locally}");

    // In a service, stop and join producer tasks before this settlement barrier.
    let report = telemetry.shutdown().await?;
    if report.outcome != AppendDeliveryOutcome::Ok {
        eprintln!("telemetry loss or ambiguity: {report:?}");
    }
    println!("final stream stats: {:?}", telemetry.stats());

    Ok(())
}
