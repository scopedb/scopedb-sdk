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

package tests

import (
	"context"
	"os"
	"strings"

	"github.com/lucasepe/codename"
	scopedb "github.com/scopedb/scopedb-sdk/go"
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
