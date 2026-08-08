/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scopedb

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var fixedDurationPattern = regexp.MustCompile(`^(-)?PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)(?:\.(\d{1,9}))?S)?$`)

// Value stores the contents of a single cell from a ScopeDB statement result.
type Value any

// ResultSet stores the result of a statement execution.
type ResultSet struct {
	// TotalRows is the total number of rows in the result set.
	TotalRows uint64
	// Schema is the schema of the result set.
	Schema Schema

	rows json.RawMessage
}

// RawRows returns the string-or-null cells from the JSON wire response without
// converting them to ScopeDB value types.
func (rs *ResultSet) RawRows() ([][]*string, error) {
	var rows [][]*string
	if err := json.Unmarshal(rs.rows, &rows); err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, errors.New("result rows must be a JSON array")
	}
	if uint64(len(rows)) != rs.TotalRows {
		return nil, fmt.Errorf(
			"result row count mismatch: expected %d, got %d",
			rs.TotalRows,
			len(rows),
		)
	}
	return rows, nil
}

// ToValues reads the result set and returns the rows as a 2D array of values,
// i.e., rows of value lists.
//
// Binary cells are returned as []byte, timestamps as time.Time, and intervals
// as time.Duration. Array, object, and any cells remain JSON strings, while
// null cells are returned as nil.
//
// This method is only valid if the result set is of the JSON format.
func (rs *ResultSet) ToValues() ([][]Value, error) {
	rows, err := rs.RawRows()
	if err != nil {
		return nil, err
	}

	valueLists := make([][]Value, 0, len(rows))
	for _, r := range rows {
		values, err := rs.convertRow(r)
		if err != nil {
			return nil, err
		}
		valueLists = append(valueLists, values)
	}
	return valueLists, nil
}

// ToObjects returns rows keyed by result column name.
//
// Duplicate output column names are rejected because representing them as a
// map would silently discard values.
func (rs *ResultSet) ToObjects() ([]map[string]Value, error) {
	names, err := rs.objectColumnNames()
	if err != nil {
		return nil, err
	}
	rows, err := rs.RawRows()
	if err != nil {
		return nil, err
	}

	objects := make([]map[string]Value, 0, len(rows))
	for _, row := range rows {
		values, err := rs.convertRow(row)
		if err != nil {
			return nil, err
		}
		object := make(map[string]Value, len(names))
		for i, name := range names {
			object[name] = values[i]
		}
		objects = append(objects, object)
	}
	return objects, nil
}

// First returns the first row keyed by column name. The boolean is false when
// the result set is empty.
func (rs *ResultSet) First() (map[string]Value, bool, error) {
	rows, err := rs.RawRows()
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}

	names, err := rs.objectColumnNames()
	if err != nil {
		return nil, false, err
	}
	values, err := rs.convertRow(rows[0])
	if err != nil {
		return nil, false, err
	}
	object := make(map[string]Value, len(names))
	for i, name := range names {
		object[name] = values[i]
	}
	return object, true, nil
}

func (rs *ResultSet) objectColumnNames() ([]string, error) {
	names := make([]string, len(rs.Schema))
	seen := make(map[string]struct{}, len(rs.Schema))
	for i, field := range rs.Schema {
		if field == nil {
			return nil, fmt.Errorf("result schema field %d is nil", i)
		}
		if _, exists := seen[field.Name]; exists {
			return nil, fmt.Errorf("duplicate result column name %q", field.Name)
		}
		seen[field.Name] = struct{}{}
		names[i] = field.Name
	}
	return names, nil
}

func (rs *ResultSet) convertRow(row []*string) ([]Value, error) {
	if len(row) != len(rs.Schema) {
		return nil, errors.New("schema length does not match record length")
	}

	values := make([]Value, 0, len(row))
	for i, cell := range row {
		field := rs.Schema[i]
		if field == nil {
			return nil, fmt.Errorf("result schema field %d is nil", i)
		}
		if cell == nil {
			values = append(values, nil)
			continue
		}
		value, err := convertValue(*cell, field.Type)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func convertValue(value string, dataType DataType) (Value, error) {
	switch dataType {
	case StringDataType:
		return value, nil
	case BinaryDataType:
		decoded, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid binary value %q: %w", value, err)
		}
		return decoded, nil
	case IntDataType:
		return strconv.ParseInt(value, 10, 64)
	case UIntDataType:
		return strconv.ParseUint(value, 10, 64)
	case FloatDataType:
		return strconv.ParseFloat(value, 64)
	case BooleanDataType:
		return strconv.ParseBool(value)
	case TimestampDataType:
		return time.Parse(time.RFC3339Nano, value)
	case IntervalDataType:
		return parseInterval(value)
	case ArrayDataType, ObjectDataType, AnyDataType:
		return value, nil
	case NullDataType:
		return nil, fmt.Errorf("unexpected non-null value for null data type: %q", value)
	default:
		return nil, fmt.Errorf("unrecognized type: %s", dataType)
	}
}

func parseInterval(value string) (time.Duration, error) {
	matches := fixedDurationPattern.FindStringSubmatch(value)
	if matches == nil || matches[2]+matches[3]+matches[4] == "" {
		return 0, fmt.Errorf("invalid interval value %q: expected fixed-duration ISO 8601 PT form", value)
	}

	duration := strings.TrimPrefix(value, "-")
	duration = strings.TrimPrefix(duration, "PT")
	duration = strings.NewReplacer("H", "h", "M", "m", "S", "s").Replace(duration)
	if matches[1] == "-" {
		duration = "-" + duration
	}

	parsed, err := time.ParseDuration(duration)
	if err != nil {
		return 0, fmt.Errorf("invalid interval value %q: %w", value, err)
	}
	return parsed, nil
}

// Schema describes the fields in a table or query result.
type Schema []*FieldSchema

// FieldSchema describes a single field.
type FieldSchema struct {
	// Name is the field name.
	Name string
	// Type is the field data type.
	Type DataType
}

// DataType is the type of field.
type DataType string

const (
	// StringDataType indicates the data is of string data type.
	StringDataType DataType = "string"
	// BinaryDataType indicates the data is of binary data type.
	BinaryDataType DataType = "binary"
	// IntDataType indicates the data is of int data type.
	IntDataType DataType = "int"
	// UIntDataType indicates the data is of uint data type.
	UIntDataType DataType = "uint"
	// FloatDataType indicates the data is of float data type.
	FloatDataType DataType = "float"
	// BooleanDataType indicates the data is of bool data type.
	BooleanDataType DataType = "boolean"
	// TimestampDataType indicates the data is of timestamp data type.
	TimestampDataType DataType = "timestamp"
	// IntervalDataType indicates the data is of interval data type.
	IntervalDataType DataType = "interval"
	// ArrayDataType indicates the data is of array data type.
	ArrayDataType DataType = "array"
	// ObjectDataType indicates the data is of object data type.
	ObjectDataType DataType = "object"
	// AnyDataType indicates the data is of any data type.
	AnyDataType DataType = "any"
	// NullDataType indicates the data is of null data type.
	NullDataType DataType = "null"
)

// UnmarshalJSON accepts the canonical wire names and normalizes the historical
// unsigned-integer alias to UIntDataType.
func (d *DataType) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("invalid data type: %w", err)
	}

	switch DataType(value) {
	case StringDataType,
		BinaryDataType,
		IntDataType,
		FloatDataType,
		BooleanDataType,
		TimestampDataType,
		IntervalDataType,
		ArrayDataType,
		ObjectDataType,
		AnyDataType,
		NullDataType:
		*d = DataType(value)
	case UIntDataType, DataType("u_int"):
		*d = UIntDataType
	default:
		return fmt.Errorf("unrecognized data type %q", value)
	}
	return nil
}

func (d DataType) valid() bool {
	switch d {
	case StringDataType,
		BinaryDataType,
		IntDataType,
		UIntDataType,
		FloatDataType,
		BooleanDataType,
		TimestampDataType,
		IntervalDataType,
		ArrayDataType,
		ObjectDataType,
		AnyDataType,
		NullDataType:
		return true
	default:
		return false
	}
}
