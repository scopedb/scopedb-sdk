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

#![allow(dead_code)]

use std::io;

use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use scopedb_client::Client;
use scopedb_client::Table;

pub fn endpoint() -> String {
    std::env::var("SCOPEDB_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6543".to_string())
}

pub fn database() -> String {
    std::env::var("SCOPEDB_DATABASE").unwrap_or_else(|_| "scopedb".to_string())
}

pub fn schema() -> String {
    std::env::var("SCOPEDB_SCHEMA").unwrap_or_else(|_| "public".to_string())
}

pub fn client() -> Result<Client, Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    if let Ok(token) = std::env::var("SCOPEDB_TOKEN") {
        if !token.is_empty() {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))?,
            );
        }
    }

    let http_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;
    Ok(Client::new(endpoint(), http_client)?)
}

pub fn write_table(client: &Client) -> Result<Table, io::Error> {
    let table = std::env::var("SCOPEDB_TABLE")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "set SCOPEDB_TABLE to a disposable table before running this write example",
            )
        })?;

    Ok(client
        .table(table)
        .with_database(database())
        .with_schema(schema()))
}
