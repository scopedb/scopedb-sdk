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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let target = common::write_table(&client)?;

    // Prefer Table::append or Table::append_stream when records already have
    // the target shape. IngestStream is useful when each record needs a SQL
    // transformation before insertion.
    let stream = client
        .ingest_stream(format!(
            r#"
            SELECT
                $0["ts"]::timestamp as occurred_at,
                $0["name"]::string as name
            INSERT INTO {} (occurred_at, name)
            "#,
            target.identifier()
        ))
        .batch_bytes(1024 * 1024)
        .flush_interval(Duration::from_millis(250))
        .build();

    stream
        .send(&serde_json::json!({
            "ts": "2026-03-13T12:00:00Z",
            "name": "ScopeDB",
        }))
        .await?;

    let flushed = stream.flush().await?;
    println!("flush result: {flushed:?}");

    let final_flush = stream.shutdown().await?;
    println!("shutdown result: {final_flush:?}");

    Ok(())
}
