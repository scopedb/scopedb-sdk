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

use scopedb_client::AppendState;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let table = common::write_table(&client)?;

    // The default Stop policy gives strict barriers. Local memory and request
    // concurrency are bounded independently; concurrent batches have no
    // defined commit order.
    let stream = table
        .append_stream()
        .target_batch_bytes(32 * 1024)
        .max_batch_rows(10_000)
        .flush_interval(Duration::from_secs(1))
        .max_buffered_bytes(1024 * 1024)
        .max_concurrent_batches(4)
        .attempt_timeout(Duration::from_secs(30))
        .build()?;

    let rows = (1..=10_000).map(|id| {
        serde_json::json!({
            "id": id,
            "occurred_at": jiff::Timestamp::now(),
            "name": format!("example-{id}"),
        })
    });

    let admission_error = match stream.send_all(rows).await {
        Ok(result) => {
            println!("accepted locally: {} rows", result.accepted_rows);
            None
        }
        Err(error) => {
            // There is no stream-wide abort or rollback. Settle the already
            // accepted prefix even if row production or admission stopped.
            eprintln!("row admission stopped; settling the accepted prefix: {error}");
            Some(error)
        }
    };

    match stream.shutdown().await {
        Ok(report) => {
            println!("committed remotely: {} rows", report.committed_rows);
            println!("final stream stats: {:?}", stream.stats());
        }
        Err(error) => {
            if matches!(
                error.append_details().map(|details| details.append_state),
                Some(AppendState::Unknown)
            ) {
                eprintln!("commit outcome is unknown; reconcile before replaying");
            } else {
                eprintln!("bulk append failed: {error}");
            }
            eprintln!("final stream stats: {:?}", stream.stats());
            return Err(error.into());
        }
    }

    if let Some(error) = admission_error {
        return Err(error.into());
    }
    Ok(())
}
