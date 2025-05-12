package integration_tests

import (
	"context"
	"fmt"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableSchema(t *testing.T) {
	c := NewClient()
	if c == nil {
		t.Skip("nil test client")
	}
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE %s (
			i INT,
			u UINT,
			f FLOAT,
			s STRING,
			b BOOLEAN,
			ts TIMESTAMP,
			var VARIANT,
		)
	`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tbl.Drop(ctx))
	}()

	schema, err := tbl.TableSchema(ctx)
	require.NoError(t, err)
	snaps.MatchSnapshot(t, schema)
}
