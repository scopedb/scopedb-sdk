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

use scopedb_client::CatalogListOptions;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;
    let database = common::database();
    let schema = common::schema();

    let mut page_token = None;
    loop {
        let page = client
            .list_databases(CatalogListOptions {
                page_size: Some(100),
                page_token,
            })
            .await?;
        for item in page.items {
            println!(
                "database {}: {}",
                item.name,
                item.comment.unwrap_or_default()
            );
        }
        page_token = page.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    let database_resource = client.fetch_database(&database).await?;
    println!("selected database: {database_resource:?}");

    let schemas = client
        .list_schemas(&database, CatalogListOptions::default())
        .await?;
    for item in schemas.items {
        println!("schema {}.{}", item.database, item.name);
    }

    let schema_resource = client.fetch_schema(&database, &schema).await?;
    println!("selected schema: {schema_resource:?}");

    let tables = client
        .list_tables(&database, &schema, CatalogListOptions::default())
        .await?;
    if let Some(first) = tables.items.first() {
        println!("first table summary: {first:?}");
        let resource = client.fetch_table(&database, &schema, &first.name).await?;
        println!("first table resource: {resource:?}");
    }

    Ok(())
}
