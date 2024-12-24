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

package integration_tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/lucasepe/codename"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// LoadConfig loads the configuration from environment variables.
func LoadConfig() *scopedb.Config {
	endpoint := os.Getenv("SCOPEDB_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	return &scopedb.Config{
		Endpoint: endpoint,
	}
}

// OptionEnabled returns true if the environment variable is set to a truthy value.
func OptionEnabled(key string) bool {
	value := os.Getenv(key)
	switch strings.ToLower(value) {
	case "1", "true", "y", "yes", "on":
		return true
	default:
		return false
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
		Statement: fmt.Sprintf(`DROP TABLE %s`, tableName),
		Format:    scopedb.ArrowJSONFormat,
	})
}
