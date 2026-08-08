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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResultSetToValuesConvertsBinaryAndInterval(t *testing.T) {
	t.Parallel()

	rows, err := json.Marshal([][]*string{{
		stringPointer("0001ABFF"),
		stringPointer(""),
		stringPointer("abcdef"),
		stringPointer("PT1H2M3.000000004S"),
		stringPointer("-PT1M0.000000001S"),
		nil,
	}})
	require.NoError(t, err)

	resultSet := &ResultSet{
		TotalRows: 1,
		Schema: Schema{
			&FieldSchema{Name: "payload", Type: BinaryDataType},
			&FieldSchema{Name: "empty_payload", Type: BinaryDataType},
			&FieldSchema{Name: "lowercase_payload", Type: BinaryDataType},
			&FieldSchema{Name: "elapsed", Type: IntervalDataType},
			&FieldSchema{Name: "offset", Type: IntervalDataType},
			&FieldSchema{Name: "nothing", Type: NullDataType},
		},
		rows: rows,
	}

	values, err := resultSet.ToValues()
	require.NoError(t, err)
	require.Equal(t, [][]Value{{
		[]byte{0x00, 0x01, 0xab, 0xff},
		[]byte{},
		[]byte{0xab, 0xcd, 0xef},
		time.Hour + 2*time.Minute + 3*time.Second + 4*time.Nanosecond,
		-time.Minute - time.Nanosecond,
		nil,
	}}, values)
}

func TestResultSetRawRowsReturnsWireCells(t *testing.T) {
	t.Parallel()

	resultSet := &ResultSet{
		TotalRows: 2,
		Schema: Schema{
			&FieldSchema{Name: "value", Type: StringDataType},
			&FieldSchema{Name: "optional", Type: StringDataType},
		},
		rows: json.RawMessage(`[["first",null],["second","present"]]`),
	}

	rows, err := resultSet.RawRows()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "first", *rows[0][0])
	require.Nil(t, rows[0][1])
	require.Equal(t, "second", *rows[1][0])
	require.Equal(t, "present", *rows[1][1])
}

func TestResultSetToObjectsKeysConvertedValues(t *testing.T) {
	t.Parallel()

	resultSet := &ResultSet{
		TotalRows: 1,
		Schema: Schema{
			&FieldSchema{Name: "signed", Type: IntDataType},
			&FieldSchema{Name: "unsigned", Type: UIntDataType},
			&FieldSchema{Name: "at", Type: TimestampDataType},
			&FieldSchema{Name: "elapsed", Type: IntervalDataType},
			&FieldSchema{Name: "payload", Type: BinaryDataType},
		},
		rows: json.RawMessage(`[[
			"-7",
			"9",
			"2026-08-08T00:00:00.123456789Z",
			"PT1.000000002S",
			"00ff"
		]]`),
	}

	objects, err := resultSet.ToObjects()
	require.NoError(t, err)
	require.Equal(t, []map[string]Value{{
		"signed":   int64(-7),
		"unsigned": uint64(9),
		"at":       time.Date(2026, 8, 8, 0, 0, 0, 123456789, time.UTC),
		"elapsed":  time.Second + 2*time.Nanosecond,
		"payload":  []byte{0x00, 0xff},
	}}, objects)
}

func TestResultSetFirstOnlyConvertsFirstRow(t *testing.T) {
	t.Parallel()

	resultSet := &ResultSet{
		TotalRows: 2,
		Schema:    Schema{&FieldSchema{Name: "value", Type: IntDataType}},
		rows:      json.RawMessage(`[["1"],["not-an-integer"]]`),
	}

	first, ok, err := resultSet.First()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[string]Value{"value": int64(1)}, first)

	_, err = resultSet.ToObjects()
	require.Error(t, err, "converting every row must still surface the invalid second row")
}

func TestResultSetFirstReportsEmptyResult(t *testing.T) {
	t.Parallel()

	resultSet := &ResultSet{
		Schema: Schema{&FieldSchema{Name: "value", Type: StringDataType}},
		rows:   json.RawMessage(`[]`),
	}

	first, ok, err := resultSet.First()
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, first)
}

func TestResultSetObjectConversionsRejectDuplicateColumns(t *testing.T) {
	t.Parallel()

	resultSet := &ResultSet{
		TotalRows: 1,
		Schema: Schema{
			&FieldSchema{Name: "value", Type: IntDataType},
			&FieldSchema{Name: "value", Type: IntDataType},
		},
		rows: json.RawMessage(`[["1","2"]]`),
	}

	_, err := resultSet.ToObjects()
	require.ErrorContains(t, err, `duplicate result column name "value"`)
	_, _, err = resultSet.First()
	require.ErrorContains(t, err, `duplicate result column name "value"`)
}

func TestResultSetToValuesConvertsIntervalBoundaries(t *testing.T) {
	t.Parallel()

	tests := map[string]time.Duration{
		"PT0S":                        0,
		"PT24H":                       24 * time.Hour,
		"PT2562047H47M16.854775807S":  time.Duration(1<<63 - 1),
		"-PT2562047H47M16.854775808S": time.Duration(-1 << 63),
	}
	for value, expected := range tests {
		value, expected := value, expected
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			resultSet := resultSetWithSingleValue(IntervalDataType, value)
			values, err := resultSet.ToValues()
			require.NoError(t, err)
			require.Equal(t, expected, values[0][0])
		})
	}
}

func TestResultSetToValuesRejectsInvalidBinary(t *testing.T) {
	t.Parallel()

	resultSet := resultSetWithSingleValue(BinaryDataType, "not-hex")

	_, err := resultSet.ToValues()
	require.ErrorContains(t, err, `invalid binary value "not-hex"`)
}

func TestResultSetToValuesRejectsInvalidInterval(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"1h", "P1D", "PT", "PT1.0000000000S", "PT2562048H"} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			resultSet := resultSetWithSingleValue(IntervalDataType, value)
			_, err := resultSet.ToValues()
			require.ErrorContains(t, err, "invalid interval value")
		})
	}
}

func TestResultSetToValuesRejectsNonNullValueForNullType(t *testing.T) {
	t.Parallel()

	resultSet := resultSetWithSingleValue(NullDataType, "null")

	_, err := resultSet.ToValues()
	require.ErrorContains(t, err, "unexpected non-null value for null data type")
}

func TestDataTypeUnmarshalAcceptsCanonicalUIntAndHistoricalAlias(t *testing.T) {
	t.Parallel()

	for _, wireValue := range []string{`"uint"`, `"u_int"`} {
		var dataType DataType
		require.NoError(t, json.Unmarshal([]byte(wireValue), &dataType))
		require.Equal(t, UIntDataType, dataType)
	}
}

func TestDataTypeUnmarshalRejectsUnknownAndNonStringValues(t *testing.T) {
	t.Parallel()

	for _, wireValue := range []string{`"unsigned"`, `42`} {
		var dataType DataType
		require.Error(t, json.Unmarshal([]byte(wireValue), &dataType))
	}
}

func resultSetWithSingleValue(dataType DataType, value string) *ResultSet {
	rows, err := json.Marshal([][]*string{{stringPointer(value)}})
	if err != nil {
		panic(err)
	}
	return &ResultSet{
		TotalRows: 1,
		Schema:    Schema{&FieldSchema{Name: "value", Type: dataType}},
		rows:      rows,
	}
}

func stringPointer(value string) *string {
	return &value
}
