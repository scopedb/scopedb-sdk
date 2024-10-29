package tests

import (
	"context"
	scopedb "github.com/scopedb/scopedb-sdk/go/v0"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadInformationSchemas(t *testing.T) {
	config := LoadConfig()
	if config == nil {
		t.Skip("Connection config is not set")
	}

	tableName, err := GenerateTableName()
	require.NoError(t, err)
	t.Logf("With tableName: %s", tableName)

	conn := scopedb.Open(config)
	rs, err := conn.QueryAsArrowBatch(context.Background(), &scopedb.StatementRequest{
		Statement:   "read information_schema.tables",
		WaitTimeout: "60s",
		Format:      scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	require.Equal(t, []*scopedb.ResultSetField{
		{Name: "database_name", DataType: "string"},
		{Name: "schema_name", DataType: "string"},
		{Name: "table_name", DataType: "string"},
		{Name: "comment", DataType: "string"},
	}, rs.Metadata.Fields)
}
