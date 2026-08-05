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

use crate::Client;
use crate::Error;
use crate::FieldSchema;
use crate::Schema;
use crate::append_stream::AppendStreamBuilder;
use crate::protocol::AppendRowsResult;

#[derive(Debug, Clone)]
pub struct Table {
    client: Client,
    database: Option<String>,
    schema: Option<String>,
    table: String,
}

impl Table {
    pub(crate) fn new(client: Client, table: String) -> Self {
        Self {
            client,
            database: None,
            schema: None,
            table,
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn identifier(&self) -> String {
        let mut parts = Vec::with_capacity(3);
        if let Some(database) = &self.database {
            parts.push(quote_ident(database, '`'));
        }
        if let Some(schema) = &self.schema {
            parts.push(quote_ident(schema, '`'));
        }
        parts.push(quote_ident(&self.table, '`'));
        parts.join(".")
    }

    pub async fn drop(&self) -> Result<(), Error> {
        self.client
            .statement(format!("DROP TABLE {}", self.identifier()))
            .execute()
            .await
            .map(|_| ())
    }

    /// Appends newline-delimited JSON rows to this table.
    pub async fn append(&self, ndjson: impl Into<String>) -> Result<AppendRowsResult, Error> {
        self.client
            .append_rows(
                self.database.as_deref().unwrap_or("scopedb"),
                self.schema.as_deref().unwrap_or("public"),
                &self.table,
                ndjson,
            )
            .await
    }

    /// Builds an asynchronous, bounded and concurrent append stream for this table.
    pub fn append_stream(&self) -> AppendStreamBuilder {
        AppendStreamBuilder::new(
            self.client.clone(),
            self.database
                .clone()
                .unwrap_or_else(|| "scopedb".to_string()),
            self.schema.clone().unwrap_or_else(|| "public".to_string()),
            self.table.clone(),
        )
    }

    pub async fn table_schema(&self) -> Result<Schema, Error> {
        let database_name = self.database.as_deref().unwrap_or("scopedb");
        let schema_name = self.schema.as_deref().unwrap_or("public");
        let table = self
            .client
            .fetch_table(database_name, schema_name, &self.table)
            .await?;
        let fields = table
            .columns
            .into_iter()
            .map(|column| FieldSchema {
                name: column.name,
                data_type: column.data_type,
            })
            .collect();

        Ok(Schema { fields })
    }
}

fn quote_ident(input: &str, quote: char) -> String {
    quote_scopeql(input, quote)
}

fn quote_scopeql(input: &str, quote: char) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push(quote);
    for ch in input.chars() {
        match ch {
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if c < '\u{20}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::quote_ident;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("plain", '`'), "`plain`");
        assert_eq!(quote_ident("a`b", '`'), "`a\\`b`");
        assert_eq!(quote_ident("a\nb", '`'), "`a\\nb`");
    }
}
