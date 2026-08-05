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

use scopedb_client::AppendState;
use scopedb_client::ErrorKind;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let table = common::write_table(&client)?;

    // Direct append sends exactly one caller-owned NDJSON request. Each value
    // is serialized as one line; JSON arrays are not accepted by this API.
    let ndjson = [
        serde_json::json!({
            "id": 1,
            "occurred_at": jiff::Timestamp::now(),
            "name": "first",
        }),
        serde_json::json!({
            "id": 2,
            "occurred_at": jiff::Timestamp::now(),
            "name": "second",
        }),
    ]
    .iter()
    .map(serde_json::to_string)
    .collect::<Result<Vec<_>, _>>()?
    .join("\n");

    match table.append(ndjson).await {
        Ok(result) => {
            println!("committed remotely: {} rows", result.num_rows_inserted);
            Ok(())
        }
        Err(error) => {
            if error.kind() == ErrorKind::AppendRowsFailed
                && matches!(
                    error.append_details().map(|details| details.append_state),
                    Some(AppendState::Unknown)
                )
            {
                eprintln!("commit outcome is unknown; reconcile before replaying this payload");
            }
            Err(error.into())
        }
    }
}
