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
	"os"
	"strings"
	"testing"

	"github.com/lucasepe/codename"
	scopedb "github.com/scopedb/scopedb-sdk/go"
	"github.com/stretchr/testify/require"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func NewClient(t testing.TB) *scopedb.Client {
	endpoint := os.Getenv("SCOPEDB_ENDPOINT")

	if endpoint == "" {
		t.Skip("SCOPEDB_ENDPOINT not set")
		return nil // unreachable
	}

	return scopedb.NewClient(&scopedb.Config{
		Endpoint: endpoint,
	})
}

func RandomName(t testing.TB) string {
	rng, err := codename.DefaultRNG()
	require.NoError(t, err)
	return strings.ReplaceAll(codename.Generate(rng, 10), "-", "_")
}
