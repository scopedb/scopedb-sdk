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
		Schema: Schema{
			&FieldSchema{Name: "payload", Type: BinaryDataType},
			&FieldSchema{Name: "empty_payload", Type: BinaryDataType},
			&FieldSchema{Name: "lowercase_payload", Type: BinaryDataType},
			&FieldSchema{Name: "elapsed", Type: IntervalDataType},
			&FieldSchema{Name: "offset", Type: IntervalDataType},
			&FieldSchema{Name: "nothing", Type: NullDataType},
		},
		Format: ResultFormatJSON,
		rows:   rows,
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

func resultSetWithSingleValue(dataType DataType, value string) *ResultSet {
	rows, err := json.Marshal([][]*string{{stringPointer(value)}})
	if err != nil {
		panic(err)
	}
	return &ResultSet{
		Schema: Schema{&FieldSchema{Name: "value", Type: dataType}},
		Format: ResultFormatJSON,
		rows:   rows,
	}
}

func stringPointer(value string) *string {
	return &value
}
