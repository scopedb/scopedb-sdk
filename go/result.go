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

	"github.com/apache/arrow/go/v17/arrow"
)

type ResultSet struct {
	TotalRows uint64

	Schema Schema
	Format ResultFormat

	rows json.RawMessage
}

func (rs *ResultSet) ToArrowBatch() ([]arrow.Record, error) {
	if rs.Format != ResultFormatArrow {
		return nil, errors.New("result set format is not Arrow")
	}

	var rows string
	err := json.Unmarshal(rs.rows, &rows)
	if err != nil {
		return nil, err
	}
	return decodeArrowBatches([]byte(rows))
}

type Schema []*FieldSchema

type FieldSchema struct {
	Name string
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
	// BoolDataType is a bool data type.
	BoolDataType DataType = "bool"
	// TimestampDataType is a timestamp data type.
	TimestampDataType DataType = "timestamp"
	// IntervalDataType is an interval data type.
	IntervalDataType DataType = "interval"
	// VariantDataType is a variant data type.
	VariantDataType DataType = "variant"
)
