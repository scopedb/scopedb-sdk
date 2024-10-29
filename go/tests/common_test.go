package tests

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/gkampitakis/go-snaps/snaps"
	scopedb "github.com/scopedb/scopedb-sdk/go/v0"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadAfterWrite(t *testing.T) {
	ctx := context.Background()

	config := LoadConfig()
	if config == nil {
		t.Skip("Connection config is not set")
	}

	tableName, err := GenerateTableName()
	require.NoError(t, err)
	t.Logf("With tableName: %s", tableName)

	conn := scopedb.Open(config)

	statement := fmt.Sprintf(`
create table %s (a int, v variant)
with (
	'storage.type' = 's3',
	'storage.endpoint'='http://minio-server:9000',
	'storage.bucket'='test-bucket',
	'storage.region'='us-east-1',
	'storage.access_key_id' = 'minioadmin',
	'storage.secret_access_key' = 'minioadmin',
);
`, tableName)
	err = conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	defer func() {
		err := DropTable(ctx, conn, tableName)
		require.NoError(t, err)
	}()

	// 1. Simple ingest and verify the result
	schema := makeSchema()
	records := makeRecords(schema)
	ingestId, err := conn.CreateIngestChannel(ctx, "scopedb", "public", tableName, nil)
	require.NoError(t, err)
	require.NoError(t, conn.IngestData(ctx, ingestId, records))
	require.NoError(t, conn.CommitIngestChannel(ctx, ingestId))

	statement = fmt.Sprintf("from %s", tableName)
	rs, err := conn.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	snaps.MatchSnapshot(t, rs.Metadata)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", rs.Records))

	// 2. Merge data and verify the result
	mergeRecords := makeMergeRecords(schema)
	mergeOptions := &scopedb.MergeOption{
		SourceTableAlias: "t1",
		SourceTableColumnNames: []string{
			"a",
			"s",
		},
		MatchCondition: tableName + ".a = t1.a",
		When: []scopedb.MergeAction{
			{
				Matched: true,
				Then:    "update_all",
			},
		},
	}
	ingestId, err = conn.CreateIngestChannel(ctx, "scopedb", "public", tableName, mergeOptions)
	require.NoError(t, err)
	require.NoError(t, conn.IngestData(ctx, ingestId, mergeRecords))
	require.NoError(t, conn.CommitIngestChannel(ctx, ingestId))

	rs, err = conn.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
		Statement: statement,
		Format:    scopedb.ArrowJSONFormat,
	})
	require.NoError(t, err)
	snaps.MatchSnapshot(t, rs.Metadata)
	snaps.MatchSnapshot(t, fmt.Sprintf("%v", rs.Records))
}

func makeSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.StructOf(arrow.Field{
			Name: "int", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		})},
	}, nil)
}

func makeRecords(schema *arrow.Schema) []arrow.Record {
	// Data:
	// a:int64 | v:struct<int:int64>
	// --------+--------------------
	// 1       | { int: 1 }
	// 2       | { int: 2 }
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).Append(true)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).Append(true)
	return []arrow.Record{b.NewRecord()}
}

func makeMergeRecords(schema *arrow.Schema) []arrow.Record {
	// Merge data:
	// a:int64 | s:struct<int:int64>
	// --------+--------------------
	// 1       | { int: 2 }
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.StructBuilder).Append(true)
	return []arrow.Record{b.NewRecord()}
}
