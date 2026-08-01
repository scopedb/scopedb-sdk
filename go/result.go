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
	// Format is the result format of the result set.
	Format ResultFormat

	rows json.RawMessage
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
	if rs.Format != ResultFormatJSON {
		return nil, fmt.Errorf("unexpected result set format: %s", rs.Format)
	}

	var rows [][]*string
	if err := json.Unmarshal(rs.rows, &rows); err != nil {
		return nil, err
	}

	convertValue := func(v string, typ DataType) (Value, error) {
		switch typ {
		case StringDataType:
			return v, nil
		case BinaryDataType:
			value, err := hex.DecodeString(v)
			if err != nil {
				return nil, fmt.Errorf("invalid binary value %q: %w", v, err)
			}
			return value, nil
		case IntDataType:
			return strconv.ParseInt(v, 10, 64)
		case UIntDataType:
			return strconv.ParseUint(v, 10, 64)
		case FloatDataType:
			return strconv.ParseFloat(v, 64)
		case BooleanDataType:
			return strconv.ParseBool(v)
		case TimestampDataType:
			return time.Parse(time.RFC3339Nano, v)
		case IntervalDataType:
			return parseInterval(v)
		case ArrayDataType, ObjectDataType, AnyDataType:
			// represent as JSON string
			return v, nil
		case NullDataType:
			return nil, fmt.Errorf("unexpected non-null value for null data type: %q", v)
		default:
			return nil, fmt.Errorf("unrecognized type: %s", typ)
		}
	}

	var valueLists [][]Value
	for _, r := range rows {
		if len(r) != len(rs.Schema) {
			return nil, errors.New("schema length does not match record length")
		}

		var values []Value
		for i, v := range r {
			fs := rs.Schema[i]
			if v == nil {
				values = append(values, nil)
			} else {
				val, err := convertValue(*v, fs.Type)
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
		}
		valueLists = append(valueLists, values)
	}
	return valueLists, nil
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
