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

use std::fmt;
use std::str::FromStr;

use crate::DataType;
use crate::Error;
use crate::ErrorKind;
use crate::ResultFormat;
use crate::protocol::ResultSetData;
use crate::protocol::StatementResultSet;

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<FieldSchema>,
}

impl Schema {
    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }
}

#[derive(Debug, Clone)]
pub struct FieldSchema {
    name: String,
    data_type: DataType,
}

impl FieldSchema {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

#[derive(Debug, Clone)]
pub struct ResultSet {
    schema: Schema,
    num_rows: usize,
    data: ResultSetData,
}

impl ResultSet {
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn format(&self) -> ResultFormat {
        self.data.format()
    }

    pub fn into_values(self) -> Result<Vec<Vec<Value>>, Error> {
        let rows = match self.data {
            ResultSetData::Json { rows } => rows,
        };

        let num_rows = self.num_rows;
        let num_fields = self.schema.fields.len();
        assert_eq!(rows.len(), num_rows);

        let mut values = Vec::with_capacity(num_rows);
        for row in rows {
            assert_eq!(row.len(), num_fields);

            let mut value_row = Vec::with_capacity(num_fields);
            for (i, cell) in row.into_iter().enumerate() {
                let Some(value) = cell else {
                    value_row.push(Value::Null);
                    continue;
                };

                let value = match self.schema.fields[i].data_type() {
                    DataType::Int => Value::Int(i64::from_str(&value).map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to parse int value: {err}"),
                        )
                    })?),
                    DataType::UInt => Value::UInt(u64::from_str(&value).map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to parse uint value: {err}"),
                        )
                    })?),
                    DataType::Float => Value::Float(f64::from_str(&value).map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to parse float value: {err}"),
                        )
                    })?),
                    DataType::Timestamp => {
                        Value::Timestamp(jiff::Timestamp::from_str(&value).map_err(|err| {
                            Error::new(
                                ErrorKind::Unexpected,
                                format!("failed to parse timestamp value: {err}"),
                            )
                        })?)
                    }
                    DataType::Interval => {
                        Value::Interval(jiff::SignedDuration::from_str(&value).map_err(|err| {
                            Error::new(
                                ErrorKind::Unexpected,
                                format!("failed to parse interval value: {err}"),
                            )
                        })?)
                    }
                    DataType::Boolean => Value::Boolean(bool::from_str(&value).map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to parse boolean value: {err}"),
                        )
                    })?),
                    DataType::String => Value::String(value),
                    DataType::Binary => Value::Binary(value),
                    DataType::Array => Value::Array(value),
                    DataType::Object => Value::Object(value),
                    DataType::Any => Value::Any(value),
                    DataType::Null => unreachable!("null values must be None in rows"),
                };
                value_row.push(value);
            }
            values.push(value_row);
        }
        Ok(values)
    }

    pub(crate) fn from_statement_result_set(result_set: StatementResultSet) -> ResultSet {
        ResultSet {
            num_rows: result_set.metadata.num_rows,
            schema: Schema {
                fields: result_set
                    .metadata
                    .fields
                    .into_iter()
                    .map(|field| FieldSchema {
                        name: field.name,
                        data_type: field.data_type,
                    })
                    .collect(),
            },
            data: result_set.data,
        }
    }
}

#[derive(Clone)]
pub enum Value {
    /// Signed integer value.
    Int(i64),
    /// Unsigned integer value.
    UInt(u64),
    /// Float value.
    Float(f64),
    /// Timestamp value.
    Timestamp(jiff::Timestamp),
    /// Interval value.
    Interval(jiff::SignedDuration),
    /// Boolean value.
    Boolean(bool),
    /// String value.
    String(String),
    /// Binary data represented as a hex string.
    Binary(String),
    /// Array of values, represented in its string format.
    Array(String),
    /// Object represented in its string format.
    Object(String),
    /// Any value, represented in its string format.
    Any(String),
    /// Null value.
    Null,
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::UInt(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:?}"),
            Value::Timestamp(v) => write!(f, "{v}"),
            Value::Interval(v) => write!(f, "{v}"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::String(v) => quote_string(f, v, '\''),
            Value::Binary(v) => write!(f, "{}", hex::encode_upper(v)),
            Value::Array(v) => write!(f, "{v}"),
            Value::Object(v) => write!(f, "{v}"),
            Value::Any(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::UInt(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:?}"),
            Value::Timestamp(v) => write!(f, "{v}"),
            Value::Interval(v) => write!(f, "{v}"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            Value::Binary(v) => write!(f, "{}", hex::encode_upper(v)),
            Value::Array(v) => write!(f, "{v}"),
            Value::Object(v) => write!(f, "{v}"),
            Value::Any(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

fn quote_string(f: &mut fmt::Formatter<'_>, s: &str, quote: char) -> fmt::Result {
    write!(f, "{quote}")?;
    for c in s.chars() {
        match c {
            '\t' => write!(f, "\\t")?,
            '\r' => write!(f, "\\r")?,
            '\n' => write!(f, "\\n")?,
            '\\' => write!(f, "\\\\")?,
            '\x00'..='\x1F' => write!(f, "\\x{:02x}", c as u8)?,
            c => {
                if c != quote {
                    write!(f, "{c}")?
                } else {
                    write!(f, "\\{quote}")?
                }
            }
        }
    }
    write!(f, "{quote}")
}
