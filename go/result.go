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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
)

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

// ToArrowBatch reads the result set and returns the rows as Arrow batches.
//
// This method is only valid if the result set is of the Arrow format.
func (rs *ResultSet) ToArrowBatch() ([]arrow.Record, error) {
	if rs.Format != ResultFormatArrow {
		return nil, fmt.Errorf("unexpected result set format: %s", rs.Format)
	}

	var rows string
	if err := json.Unmarshal(rs.rows, &rows); err != nil {
		return nil, err
	}
	return decodeArrowBatches([]byte(rows))
}

// ToValues reads the result set and returns the rows as a 2D array of values,
// i.e., rows of value lists.
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
			return time.ParseDuration(v)
		case VariantDataType:
			return v, nil
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
	// StringDataType is a string data type.
	StringDataType DataType = "string"
	// IntDataType is an int data type.
	IntDataType DataType = "int"
	// UIntDataType is an uint data type.
	UIntDataType DataType = "uint"
	// FloatDataType is a float data type.
	FloatDataType DataType = "float"
	// BooleanDataType is a bool data type.
	BooleanDataType DataType = "boolean"
	// TimestampDataType is a timestamp data type.
	TimestampDataType DataType = "timestamp"
	// IntervalDataType is an interval data type.
	IntervalDataType DataType = "interval"
	// VariantDataType is a variant data type.
	VariantDataType DataType = "variant"
)
