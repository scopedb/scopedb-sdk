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

#[test]
fn reexported_http_client_matches_constructor() {
    scopedb_client::Client::new(
        "http://127.0.0.1:6543",
        scopedb_client::reqwest::Client::new(),
    )
    .unwrap();
}

#[test]
fn ergonomic_client_builder_is_public() {
    scopedb_client::Client::builder("http://127.0.0.1:6543")
        .api_key("test-key")
        .http_client(scopedb_client::reqwest::Client::new())
        .build()
        .unwrap();
}

#[allow(dead_code)]
fn application_api_surface_compiles(client: &scopedb_client::Client) {
    let _query = client.query("SELECT 1");
    let _databases = client.iterate_databases(scopedb_client::CatalogListOptions::default());
    let _schemas = client.iterate_schemas("scopedb", scopedb_client::CatalogListOptions::default());
    let _tables = client.iterate_tables(
        "scopedb",
        "public",
        scopedb_client::CatalogListOptions::default(),
    );
    let table = client.table("events").with_schema("public");
    let _description = table.describe();
    let _stream = table
        .append_stream()
        .target_batch_bytes(1024)
        .max_batch_rows(100)
        .max_buffered_bytes(4096)
        .max_concurrent_batches(2);
}
