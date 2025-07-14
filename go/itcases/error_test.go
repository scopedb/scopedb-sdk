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
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestFetchStatementFail(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()

	id, err := uuid.Parse("c8fe71d6-3695-11f0-85b3-063c3400fda9")
	require.NoError(t, err)
	err = c.StatementHandle(id).FetchOnce(ctx)
	require.Error(t, err)
	snaps.MatchSnapshot(t, err.Error())
}

func TestSubmitStatementFail(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()

	_, err := c.Statement("SELECT UNKNOWN_FUNCTION()").Execute(ctx)
	require.Error(t, err)
	snaps.MatchSnapshot(t, err.Error())
}
