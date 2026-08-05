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
    let table_name = std::env::var("SCOPEDB_TABLE").unwrap_or_else(|_| "events".to_string());
    let table = client
        .table(table_name)
        .with_database(common::database())
        .with_schema(common::schema());

    println!("identifier: {}", table.identifier());

    let schema = table.table_schema().await?;
    for field in schema.fields() {
        println!("{}: {:?}", field.name(), field.data_type());
    }

    Ok(())
}
