package tests

import (
	"context"
	"github.com/lucasepe/codename"
	scopedb "github.com/scopedb/scopedb-sdk/go/v0"
	"os"
	"strings"
)

// LoadConfig loads the configuration from environment variables.
func LoadConfig() *scopedb.Config {
	if os.Getenv("SCOPEDB_ENDPOINT") == "" {
		return nil
	}

	return &scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
	}
}

func GenerateTableName() (string, error) {
	rng, err := codename.DefaultRNG()
	if err != nil {
		return "", err
	}
	tableName := strings.ReplaceAll(codename.Generate(rng, 10), "-", "_")
	return tableName, nil
}

func DropTable(ctx context.Context, conn *scopedb.Connection, tableName string) error {
	return conn.Execute(ctx, &scopedb.StatementRequest{
		Statement: "drop table " + tableName,
		Format:    scopedb.ArrowJSONFormat,
	})
}
