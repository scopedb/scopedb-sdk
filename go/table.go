package scopedb

import (
	"bytes"
	"context"
	"fmt"
)

type Table struct {
	c *Client

	// Database is the name of the database.
	//
	// This is optional and may be empty.
	Database string
	// Schema is the name of the schema.
	//
	// This is optional and may be empty. When Database is not empty,
	// Schema must not be empty.
	Schema string
	// Table is the name of the table.
	Table string
}

func (c *Client) Table(tableName string) *Table {
	return &Table{
		c:     c,
		Table: tableName,
	}
}

func (t *Table) Drop(ctx context.Context) error {
	s := t.c.Statement(fmt.Sprintf(`DROP TABLE %s`, t.Identifier()))
	_, err := s.Execute(ctx)
	return err
}

func (t *Table) TableSchema(ctx context.Context) (Schema, error) {
	var dbName, schemaName, tableName string
	if t.Database != "" {
		dbName = quoteIdent(t.Database, '\'')
	} else {
		dbName = quoteIdent("scopedb", '\'')
	}
	if t.Schema != "" {
		schemaName = quoteIdent(t.Schema, '\'')
	} else {
		schemaName = quoteIdent("public", '\'')
	}
	tableName = quoteIdent(t.Table, '\'')

	r, err := t.c.Statement(fmt.Sprintf(`
		FROM scopedb.system.columns
		WHERE table_name = %s
		  AND schema_name = %s
		  AND database_name = %s
		SELECT column_name, data_type
	`, tableName, schemaName, dbName)).Execute(ctx)

	if err != nil {
		return nil, err
	}

	var records [][]Value
	if records, err = r.ToValues(); err != nil {
		return nil, err
	}
	var schema Schema
	for _, record := range records {
		if len(record) != 2 {
			return nil, fmt.Errorf("expected 2 columns, got %d", len(record))
		}
		name, ok := record[0].(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", record[0])
		}
		dataType, ok := record[1].(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", record[1])
		}
		schema = append(schema, &FieldSchema{
			Name: name,
			Type: DataType(dataType),
		})
	}
	return schema, nil
}

func (t *Table) Identifier() string {
	var b bytes.Buffer
	if t.Database != "" {
		b.WriteString(quoteIdent(t.Database, '`'))
		b.WriteByte('.')
	}
	if t.Schema != "" {
		b.WriteString(quoteIdent(t.Schema, '`'))
		b.WriteByte('.')
	}
	b.WriteString(quoteIdent(t.Table, '`'))
	return b.String()
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
