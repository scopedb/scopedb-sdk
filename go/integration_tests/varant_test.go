package integration_tests

import (
	"context"
	"fmt"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"testing"

	testkit "github.com/scopedb/scopedb-sdk/go/integration_tests/internal"
)

const (
	BatchSize  = 1024
	BatchCount = 1
)

func BenchmarkLargeVariantSchema(b *testing.B) {
	tk := testkit.NewTestKit(b)
	if tk == nil {
		b.Skip("nil testkit")
	}
	defer tk.Close()

	ctx := context.Background()
	tk.NewTable(ctx, "bench_vars", `CREATE TABLE bench_vars (b int, n int, var variant)`)

	for i := 0; i < BatchCount; i++ {
		ingest := "VALUES "
		for n := 0; n < BatchSize; n++ {
			ingest += fmt.Sprintf("(%d, %d, PARSE_JSON('{}')), ", i, n)
		}
		ingest += "INSERT INTO bench_vars"
		tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
			Statement: ingest,
			Format:    scopedb.ArrowJSONFormat,
		})
	}

	for i := 0; i < b.N; i++ {
		tk.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
			Statement: "FROM bench_vars AGGREGATE OBJECT_SCHEMA(var)",
			Format:    scopedb.ArrowJSONFormat,
		})
	}
}
