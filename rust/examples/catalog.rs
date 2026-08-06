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

    let mut databases = client.iterate_databases(CatalogListOptions {
        page_size: Some(100),
        page_token: None,
    });
    while let Some(item) = databases.next().await? {
        println!(
            "database {}: {}",
            item.name,
            item.comment.unwrap_or_default()
        );
    }

    let database_resource = client.fetch_database(&database).await?;
    println!("selected database: {database_resource:?}");

    let mut schemas = client.iterate_schemas(&database, CatalogListOptions::default());
    while let Some(item) = schemas.next().await? {
        println!("schema {}.{}", item.database, item.name);
    }

    let schema_resource = client.fetch_schema(&database, &schema).await?;
    println!("selected schema: {schema_resource:?}");

    let mut tables = client.iterate_tables(&database, &schema, CatalogListOptions::default());
    if let Some(first) = tables.next().await? {
        println!("first table summary: {first:?}");
        let resource = client.fetch_table(&database, &schema, &first.name).await?;
        println!("first table resource: {resource:?}");
    }

    Ok(())
}
