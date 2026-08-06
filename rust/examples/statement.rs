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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;

    let result = client.query("SELECT 1 AS ready").await?;
    let rows = result.into_objects()?;
    println!("rows: {rows:?}");

    // Keep the handle when an application needs progress, cancellation, or a
    // durable statement ID before waiting for the result.
    let mut handle = client.statement("SELECT 2 AS value").submit().await?;
    let statement_id = handle.statement_id();
    println!(
        "statement {statement_id} submitted: {:?}",
        handle.last_status()
    );
    let status = handle.status().await?;
    println!("statement {statement_id}: {status:?}");
    let result = handle.wait().await?;
    println!("handle rows: {:?}", result.into_objects()?);
    Ok(())
}
