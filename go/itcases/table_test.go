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

package itcases

import (
	"context"
	"fmt"
	"testing"

	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"
)

func TestTableDescribe(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE %s (
			i int,
			u uint,
			f float,
			s string,
			b boolean,
			ts timestamp,
			var any,
		)
	`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	description, err := tbl.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, "scopedb", description.Database)
	require.Equal(t, "public", description.Schema)
	require.Equal(t, tbl.Name, description.Name)
	require.Equal(t, []scopedb.TableColumnSpec{
		{Name: "i", DataType: scopedb.IntDataType},
		{Name: "u", DataType: scopedb.UIntDataType},
		{Name: "f", DataType: scopedb.FloatDataType},
		{Name: "s", DataType: scopedb.StringDataType},
		{Name: "b", DataType: scopedb.BooleanDataType},
		{Name: "ts", DataType: scopedb.TimestampDataType},
		{Name: "var", DataType: scopedb.AnyDataType},
	}, description.Columns)
}
