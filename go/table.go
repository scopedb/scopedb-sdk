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

package scopedb

import (
	"bytes"
	"context"
	"fmt"
)

// Table references a ScopeDB table.
type Table struct {
	c *Client

	// Database is the database name. Empty uses "scopedb" for REST APIs.
	Database string
	// Schema is the schema name. Empty uses "public" for REST APIs.
	Schema string
	// Name is the name of the table.
	Name string
}

// Table creates a new Table object with the given name.
func (c *Client) Table(tableName string) *Table {
	return &Table{
		c:    c,
		Name: tableName,
	}
}

// Drop drops the table from ScopeDB.
//
// This method issues a DROP TABLE statement to ScopeDB and blocks until done.
func (t *Table) Drop(ctx context.Context) error {
	s := t.c.Statement(fmt.Sprintf(`DROP TABLE %s`, t.Identifier()))
	_, err := s.Execute(ctx)
	return err
}

// Describe returns the complete REST catalog resource for this table.
func (t *Table) Describe(ctx context.Context) (TableResource, error) {
	return t.c.FetchTable(ctx, t.databaseName(), t.schemaName(), t.Name)
}

// AppendNDJSON sends one caller-encoded NDJSON request to this table. The body
// contains one JSON object per non-empty line, not a JSON array.
func (t *Table) AppendNDJSON(ctx context.Context, ndjson []byte) (AppendRowsResult, error) {
	return t.c.appendNDJSON(ctx, t.databaseName(), t.schemaName(), t.Name, ndjson)
}

// Identifier returns the quoted table identifier.
func (t *Table) Identifier() string {
	var b bytes.Buffer
	if t.Database != "" {
		b.WriteString(quoteIdent(t.Database, '`'))
		b.WriteByte('.')
		b.WriteString(quoteIdent(t.schemaName(), '`'))
		b.WriteByte('.')
	} else if t.Schema != "" {
		b.WriteString(quoteIdent(t.Schema, '`'))
		b.WriteByte('.')
	}
	b.WriteString(quoteIdent(t.Name, '`'))
	return b.String()
}

func (t *Table) databaseName() string {
	if t.Database == "" {
		return "scopedb"
	}
	return t.Database
}

func (t *Table) schemaName() string {
	if t.Schema == "" {
		return "public"
	}
	return t.Schema
}

func quoteIdent(s string, r rune) string {
	var b bytes.Buffer
	b.WriteRune(r)
	for _, c := range s {
		switch c {
		case '\t':
			b.WriteString("\\t")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\\':
			b.WriteString("\\\\")
		default:
			if c == r {
				b.WriteRune('\\')
				b.WriteRune(c)
				break
			}

			if c < 0x20 {
				b.WriteString(fmt.Sprintf("\\x%02x", c))
				break
			}

			b.WriteRune(c)
		}
	}
	b.WriteRune(r)
	return b.String()
}
