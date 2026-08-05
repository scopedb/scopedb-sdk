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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use crate::DataType;
use crate::Error;
use crate::ErrorKind;
use crate::protocol::ResultSetData;
use crate::protocol::StatementResultSet;

#[derive(Debug, Clone)]
pub struct Schema {
    pub(crate) fields: Vec<FieldSchema>,
}

impl Schema {
    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }
}

#[derive(Debug, Clone)]
pub struct FieldSchema {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
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

/// One typed result row keyed by its ScopeQL output column name.
pub type ResultObject = BTreeMap<String, Value>;

impl ResultSet {
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the unparsed JSON-format cells exactly as received from ScopeDB.
    pub fn raw_rows(&self) -> &[Vec<Option<String>>] {
        match &self.data {
            ResultSetData::Json { rows } => rows,
        }
    }

    /// Backward-compatible alias for [`ResultSet::raw_rows`].
    #[deprecated(note = "use raw_rows")]
    pub fn json_rows(&self) -> Option<&[Vec<Option<String>>]> {
        Some(self.raw_rows())
    }

    /// Converts all result cells to their typed Rust representation.
    pub fn to_values(&self) -> Result<Vec<Vec<Value>>, Error> {
        self.validate_shape()?;
        self.raw_rows()
            .iter()
            .map(|row| {
                row.iter()
                    .zip(self.schema.fields.iter())
                    .map(|(cell, field)| parse_cell(cell.as_deref(), field.data_type()))
                    .collect()
            })
            .collect()
    }

    /// Consumes the result set and returns typed rows.
    pub fn into_values(self) -> Result<Vec<Vec<Value>>, Error> {
        self.validate_shape()?;
        let ResultSet { schema, data, .. } = self;
        let rows = match data {
            ResultSetData::Json { rows } => rows,
        };
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .zip(schema.fields.iter())
                    .map(|(cell, field)| parse_owned_cell(cell, field.data_type()))
                    .collect()
            })
            .collect()
    }

    /// Converts all rows to maps keyed by unique output column names.
    ///
    /// Duplicate output names are rejected instead of silently discarding a
    /// value. Use [`ResultSet::to_values`] when a query intentionally returns
    /// duplicate column names.
    pub fn to_objects(&self) -> Result<Vec<ResultObject>, Error> {
        self.validate_unique_fields()?;
        self.to_values().map(|rows| {
            rows.into_iter()
                .map(|row| {
                    self.schema
                        .fields
                        .iter()
                        .map(|field| field.name.clone())
                        .zip(row)
                        .collect()
                })
                .collect()
        })
    }

    /// Consumes the result set and returns rows keyed by output column name.
    pub fn into_objects(self) -> Result<Vec<ResultObject>, Error> {
        self.validate_unique_fields()?;
        self.validate_shape()?;
        let ResultSet { schema, data, .. } = self;
        let names = schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        let rows = match data {
            ResultSetData::Json { rows } => rows,
        };
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .zip(schema.fields.iter())
                    .map(|(cell, field)| parse_owned_cell(cell, field.data_type()))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| names.iter().cloned().zip(values).collect())
            })
            .collect()
    }

    /// Returns the first row keyed by output column name, or `None` when empty.
    pub fn first(&self) -> Result<Option<ResultObject>, Error> {
        let Some(row) = self.raw_rows().first() else {
            return Ok(None);
        };
        self.validate_unique_fields()?;
        self.validate_row_shape(0, row)?;
        let values = row
            .iter()
            .zip(self.schema.fields.iter())
            .map(|(cell, field)| parse_cell(cell.as_deref(), field.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(
            self.schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .zip(values)
                .collect(),
        ))
    }

    fn validate_shape(&self) -> Result<(), Error> {
        let rows = self.raw_rows();
        if rows.len() != self.num_rows {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "result row count mismatch: expected {}, got {}",
                    self.num_rows,
                    rows.len()
                ),
            ));
        }
        let expected = self.schema.fields.len();
        if let Some((index, row)) = rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.len() != expected)
        {
            return self.validate_row_shape(index, row);
        }
        Ok(())
    }

    fn validate_row_shape(&self, index: usize, row: &[Option<String>]) -> Result<(), Error> {
        let expected = self.schema.fields.len();
        if row.len() == expected {
            return Ok(());
        }
        Err(Error::new(
            ErrorKind::Unexpected,
            format!(
                "result row {index} field count mismatch: expected {expected}, got {}",
                row.len()
            ),
        ))
    }

    fn validate_unique_fields(&self) -> Result<(), Error> {
        let mut names = HashSet::with_capacity(self.schema.fields.len());
        if let Some(field) = self
            .schema
            .fields
            .iter()
            .find(|field| !names.insert(field.name.as_str()))
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "cannot convert result to objects because column name {:?} is duplicated; use to_values instead",
                    field.name
                ),
            ));
        }
        Ok(())
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

fn parse_cell(value: Option<&str>, data_type: DataType) -> Result<Value, Error> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };

    match data_type {
        DataType::Int => i64::from_str(value).map(Value::Int).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to parse int value: {err}"),
            )
        }),
        DataType::UInt => u64::from_str(value).map(Value::UInt).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to parse uint value: {err}"),
            )
        }),
        DataType::Float => f64::from_str(value).map(Value::Float).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to parse float value: {err}"),
            )
        }),
        DataType::Timestamp => jiff::Timestamp::from_str(value)
            .map(Value::Timestamp)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to parse timestamp value: {err}"),
                )
            }),
        DataType::Interval => jiff::SignedDuration::from_str(value)
            .map(Value::Interval)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("failed to parse interval value: {err}"),
                )
            }),
        DataType::Boolean => bool::from_str(value).map(Value::Boolean).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to parse boolean value: {err}"),
            )
        }),
        DataType::String => Ok(Value::String(value.to_string())),
        DataType::Binary => Ok(Value::Binary(value.to_string())),
        DataType::Array => Ok(Value::Array(value.to_string())),
        DataType::Object => Ok(Value::Object(value.to_string())),
        DataType::Any => Ok(Value::Any(value.to_string())),
        DataType::Null => Err(Error::new(
            ErrorKind::Unexpected,
            "non-null cell returned for a null column",
        )),
    }
}

fn parse_owned_cell(value: Option<String>, data_type: DataType) -> Result<Value, Error> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };

    match data_type {
        DataType::String => Ok(Value::String(value)),
        DataType::Binary => Ok(Value::Binary(value)),
        DataType::Array => Ok(Value::Array(value)),
        DataType::Object => Ok(Value::Object(value)),
        DataType::Any => Ok(Value::Any(value)),
        _ => parse_cell(Some(&value), data_type),
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
            Value::Timestamp(v) => format_timestamp(f, v),
            Value::Interval(v) => format_interval(f, v),
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
            Value::Timestamp(v) => format_timestamp(f, v),
            Value::Interval(v) => format_interval(f, v),
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

fn format_timestamp(f: &mut fmt::Formatter<'_>, ts: &jiff::Timestamp) -> fmt::Result {
    use jiff::fmt::StdFmtWrite;
    use jiff::fmt::temporal;

    let precision = f.precision().map(|p| u8::try_from(p).unwrap_or(u8::MAX));
    temporal::DateTimePrinter::new()
        .precision(precision)
        .print_timestamp(ts, StdFmtWrite(f))
        .map_err(|_| fmt::Error)
}

fn format_interval(f: &mut fmt::Formatter<'_>, d: &jiff::SignedDuration) -> fmt::Result {
    use jiff::fmt::StdFmtWrite;
    use jiff::fmt::friendly;

    friendly::SpanPrinter::new()
        .spacing(friendly::Spacing::None)
        .designator(friendly::Designator::Compact)
        .print_duration(d, StdFmtWrite(f))
        .map_err(|_| fmt::Error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FieldMetadata;
    use crate::protocol::ResultSetMetadata;
    use crate::protocol::StatementResultSet;

    fn result(fields: Vec<(&str, DataType)>, rows: Vec<Vec<Option<&str>>>) -> ResultSet {
        let num_rows = rows.len();
        ResultSet::from_statement_result_set(StatementResultSet {
            metadata: ResultSetMetadata {
                fields: fields
                    .into_iter()
                    .map(|(name, data_type)| FieldMetadata {
                        name: name.to_string(),
                        data_type,
                    })
                    .collect(),
                num_rows,
            },
            data: ResultSetData::Json {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|cell| cell.map(str::to_string))
                            .collect()
                    })
                    .collect(),
            },
        })
    }

    #[test]
    fn exposes_raw_values_objects_and_first_row() {
        let result = result(
            vec![("id", DataType::Int), ("name", DataType::String)],
            vec![vec![Some("42"), Some("ScopeDB")]],
        );

        assert_eq!(result.raw_rows()[0][0].as_deref(), Some("42"));
        let values = result.to_values().unwrap();
        assert!(matches!(values[0][0], Value::Int(42)));
        let objects = result.to_objects().unwrap();
        assert!(matches!(objects[0].get("id"), Some(Value::Int(42))));
        assert!(matches!(
            result.first().unwrap().unwrap().get("name"),
            Some(Value::String(value)) if value == "ScopeDB"
        ));
    }

    #[test]
    fn consuming_conversions_move_string_values() {
        let values = result(
            vec![("name", DataType::String)],
            vec![vec![Some("ScopeDB")]],
        )
        .into_values()
        .unwrap();
        assert!(matches!(&values[0][0], Value::String(value) if value == "ScopeDB"));

        let objects = result(
            vec![("name", DataType::String)],
            vec![vec![Some("ScopeDB")]],
        )
        .into_objects()
        .unwrap();
        assert!(matches!(
            objects[0].get("name"),
            Some(Value::String(value)) if value == "ScopeDB"
        ));
    }

    #[test]
    fn malformed_shapes_return_errors_instead_of_panicking() {
        let mut malformed = result(
            vec![("id", DataType::Int)],
            vec![vec![Some("1"), Some("extra")]],
        );
        malformed.num_rows = 2;
        let error = malformed.to_values().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
    }

    #[test]
    fn object_conversion_rejects_duplicate_names() {
        let result = result(
            vec![("value", DataType::Int), ("value", DataType::Int)],
            vec![vec![Some("1"), Some("2")]],
        );
        let error = result.to_objects().unwrap_err();
        assert!(error.message().contains("duplicated"));
    }

    #[test]
    fn first_parses_only_the_first_row() {
        let result = result(
            vec![("id", DataType::Int)],
            vec![vec![Some("1")], vec![Some("2"), Some("extra")]],
        );
        assert!(matches!(
            result.first().unwrap().unwrap().get("id"),
            Some(Value::Int(1))
        ));
        assert!(result.to_values().is_err());
    }

    #[test]
    fn first_returns_none_for_empty_results_with_duplicate_names() {
        let result = result(
            vec![("value", DataType::Int), ("value", DataType::Int)],
            vec![],
        );
        assert!(result.first().unwrap().is_none());
    }
}
