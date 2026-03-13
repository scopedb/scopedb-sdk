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

use std::str::FromStr;

use crate::Client;
use crate::DataType;
use crate::Error;
use crate::ErrorKind;
use crate::FieldSchema;
use crate::Schema;
use crate::Value;

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

    pub async fn table_schema(&self) -> Result<Schema, Error> {
        let database_name = self.database.as_deref().unwrap_or("scopedb");
        let schema_name = self.schema.as_deref().unwrap_or("public");
        let statement = format!(
            r#"
            FROM scopedb.system.columns
            WHERE table_name = {}
              AND schema_name = {}
              AND database_name = {}
            SELECT column_name, data_type
            "#,
            quote_string_literal(&self.table),
            quote_string_literal(schema_name),
            quote_string_literal(database_name),
        );

        let rows = self
            .client
            .statement(statement)
            .execute()
            .await?
            .into_values()?;

        let mut fields = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != 2 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("expected 2 columns in table schema row, got {}", row.len()),
                ));
            }

            let column_name = match &row[0] {
                Value::String(value) => value.clone(),
                value => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("expected string column name, got {value:?}"),
                    ));
                }
            };
            let data_type = match &row[1] {
                Value::String(value) => DataType::from_str(value)?,
                value => {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("expected string data type, got {value:?}"),
                    ));
                }
            };

            fields.push(FieldSchema {
                name: column_name,
                data_type,
            });
        }

        Ok(Schema { fields })
    }
}

fn quote_ident(input: &str, quote: char) -> String {
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

fn quote_string_literal(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('\'');
    for ch in input.chars() {
        match ch {
            '\'' => out.push_str("''"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::quote_ident;
    use super::quote_string_literal;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("plain", '`'), "`plain`");
        assert_eq!(quote_ident("a`b", '`'), "`a\\`b`");
        assert_eq!(quote_ident("a\nb", '`'), "`a\\nb`");
    }

    #[test]
    fn test_quote_string_literal() {
        assert_eq!(quote_string_literal("plain"), "'plain'");
        assert_eq!(quote_string_literal("a'b"), "'a''b'");
        assert_eq!(quote_string_literal("a\nb"), "'a\nb'");
    }
}
