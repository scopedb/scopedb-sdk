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
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"slices"
	"strconv"
)

// CatalogListOptions configures one catalog list request.
type CatalogListOptions struct {
	// PageSize is the maximum number of resources returned in one page.
	// Zero uses the server default; otherwise the value must be from 1 to 1000.
	PageSize int
	// PageToken is the opaque continuation token returned by the previous page.
	PageToken string
}

// CatalogPage is one page of catalog resources.
type CatalogPage[T any] struct {
	Items         []T    `json:"items"`
	NextPageToken string `json:"next_page_token,omitempty"`
}

// DatabaseResource describes a database in the catalog.
type DatabaseResource struct {
	Name    string  `json:"name"`
	Comment *string `json:"comment"`
}

// SchemaResource describes a schema in the catalog.
type SchemaResource struct {
	Database string  `json:"database"`
	Name     string  `json:"name"`
	Comment  *string `json:"comment"`
}

// TableResourceSummary describes a table without its full specification.
type TableResourceSummary struct {
	Database string  `json:"database"`
	Schema   string  `json:"schema"`
	Name     string  `json:"name"`
	Comment  *string `json:"comment"`
}

// TableColumnSpec describes one table column.
type TableColumnSpec struct {
	Name     string   `json:"name"`
	DataType DataType `json:"data_type"`
	Comment  *string  `json:"comment"`
}

// TableDistinctSpec describes the table's distinct-key configuration.
type TableDistinctSpec struct {
	On []string `json:"on"`
	By []string `json:"by"`
}

// TableSpec describes the reusable portion of a table definition.
type TableSpec struct {
	Columns           []TableColumnSpec `json:"columns"`
	PartitionBy       []string          `json:"partition_by"`
	ClusterBy         []string          `json:"cluster_by"`
	DistinctOn        TableDistinctSpec `json:"distinct_on"`
	DataRetentionDays *int32            `json:"data_retention_days"`
	Comment           *string           `json:"comment"`
}

// TableResource describes a table and its complete catalog specification.
type TableResource struct {
	Database          string            `json:"database"`
	Schema            string            `json:"schema"`
	Name              string            `json:"name"`
	Columns           []TableColumnSpec `json:"columns"`
	PartitionBy       []string          `json:"partition_by"`
	ClusterBy         []string          `json:"cluster_by"`
	DistinctOn        TableDistinctSpec `json:"distinct_on"`
	DataRetentionDays *int32            `json:"data_retention_days"`
	Comment           *string           `json:"comment"`
}

type catalogResource interface {
	validateCatalogResource() error
}

func (resource DatabaseResource) validateCatalogResource() error {
	if resource.Name == "" {
		return fmt.Errorf("database resource is missing name")
	}
	return nil
}

func (resource SchemaResource) validateCatalogResource() error {
	if resource.Database == "" {
		return fmt.Errorf("schema resource is missing database")
	}
	if resource.Name == "" {
		return fmt.Errorf("schema resource is missing name")
	}
	return nil
}

func (resource TableResourceSummary) validateCatalogResource() error {
	if resource.Database == "" {
		return fmt.Errorf("table resource summary is missing database")
	}
	if resource.Schema == "" {
		return fmt.Errorf("table resource summary is missing schema")
	}
	if resource.Name == "" {
		return fmt.Errorf("table resource summary is missing name")
	}
	return nil
}

func (resource TableResource) validateCatalogResource() error {
	if resource.Database == "" {
		return fmt.Errorf("table resource is missing database")
	}
	if resource.Schema == "" {
		return fmt.Errorf("table resource is missing schema")
	}
	if resource.Name == "" {
		return fmt.Errorf("table resource is missing name")
	}
	if resource.Columns == nil {
		return fmt.Errorf("table resource is missing columns")
	}
	for index, column := range resource.Columns {
		if column.Name == "" {
			return fmt.Errorf("table resource column %d is missing name", index)
		}
		if !column.DataType.valid() {
			return fmt.Errorf("table resource column %d is missing data_type", index)
		}
	}
	if resource.PartitionBy == nil {
		return fmt.Errorf("table resource is missing partition_by")
	}
	if resource.ClusterBy == nil {
		return fmt.Errorf("table resource is missing cluster_by")
	}
	if resource.DistinctOn.On == nil {
		return fmt.Errorf("table resource is missing distinct_on.on")
	}
	if resource.DistinctOn.By == nil {
		return fmt.Errorf("table resource is missing distinct_on.by")
	}
	return nil
}

// Spec returns the reusable table specification without its catalog identity.
func (resource TableResource) Spec() TableSpec {
	columns := slices.Clone(resource.Columns)
	for i := range columns {
		columns[i].Comment = clonePointer(columns[i].Comment)
	}
	return TableSpec{
		Columns:     columns,
		PartitionBy: slices.Clone(resource.PartitionBy),
		ClusterBy:   slices.Clone(resource.ClusterBy),
		DistinctOn: TableDistinctSpec{
			On: slices.Clone(resource.DistinctOn.On),
			By: slices.Clone(resource.DistinctOn.By),
		},
		DataRetentionDays: clonePointer(resource.DataRetentionDays),
		Comment:           clonePointer(resource.Comment),
	}
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// ListDatabases returns one page of databases.
func (c *Client) ListDatabases(
	ctx context.Context,
	options CatalogListOptions,
) (CatalogPage[DatabaseResource], error) {
	return fetchCatalogPage[DatabaseResource](ctx, c, []string{"databases"}, options)
}

// FetchDatabase returns one database resource.
func (c *Client) FetchDatabase(ctx context.Context, database string) (DatabaseResource, error) {
	return fetchCatalogResource[DatabaseResource](ctx, c, "databases", database)
}

// ListSchemas returns one page of schemas in a database.
func (c *Client) ListSchemas(
	ctx context.Context,
	database string,
	options CatalogListOptions,
) (CatalogPage[SchemaResource], error) {
	return fetchCatalogPage[SchemaResource](
		ctx,
		c,
		[]string{"databases", database, "schemas"},
		options,
	)
}

// FetchSchema returns one schema resource.
func (c *Client) FetchSchema(
	ctx context.Context,
	database string,
	schema string,
) (SchemaResource, error) {
	return fetchCatalogResource[SchemaResource](ctx, c, "databases", database, "schemas", schema)
}

// ListTables returns one page of tables in a schema.
func (c *Client) ListTables(
	ctx context.Context,
	database string,
	schema string,
	options CatalogListOptions,
) (CatalogPage[TableResourceSummary], error) {
	return fetchCatalogPage[TableResourceSummary](
		ctx,
		c,
		[]string{"databases", database, "schemas", schema, "tables"},
		options,
	)
}

// FetchTable returns one table resource with its complete specification.
func (c *Client) FetchTable(
	ctx context.Context,
	database string,
	schema string,
	table string,
) (TableResource, error) {
	return fetchCatalogResource[TableResource](
		ctx,
		c,
		"databases",
		database,
		"schemas",
		schema,
		"tables",
		table,
	)
}

// IterateDatabases lazily iterates databases across all catalog pages.
func (c *Client) IterateDatabases(
	ctx context.Context,
	options CatalogListOptions,
) iter.Seq2[DatabaseResource, error] {
	return iterateCatalog(options, func(options CatalogListOptions) (CatalogPage[DatabaseResource], error) {
		return c.ListDatabases(ctx, options)
	})
}

// IterateSchemas lazily iterates schemas across all catalog pages.
func (c *Client) IterateSchemas(
	ctx context.Context,
	database string,
	options CatalogListOptions,
) iter.Seq2[SchemaResource, error] {
	return iterateCatalog(options, func(options CatalogListOptions) (CatalogPage[SchemaResource], error) {
		return c.ListSchemas(ctx, database, options)
	})
}

// IterateTables lazily iterates tables across all catalog pages.
func (c *Client) IterateTables(
	ctx context.Context,
	database string,
	schema string,
	options CatalogListOptions,
) iter.Seq2[TableResourceSummary, error] {
	return iterateCatalog(options, func(options CatalogListOptions) (CatalogPage[TableResourceSummary], error) {
		return c.ListTables(ctx, database, schema, options)
	})
}

func fetchCatalogPage[T catalogResource](
	ctx context.Context,
	c *Client,
	segments []string,
	options CatalogListOptions,
) (CatalogPage[T], error) {
	u, err := c.catalogURL(segments, options)
	if err != nil {
		return CatalogPage[T]{}, err
	}
	return fetchJSON[CatalogPage[T]](ctx, c, u, validateCatalogPage[T])
}

func fetchCatalogResource[T catalogResource](
	ctx context.Context,
	c *Client,
	segments ...string,
) (T, error) {
	var zero T
	u, err := c.resourceURL(segments...)
	if err != nil {
		return zero, err
	}
	return fetchJSON[T](ctx, c, u, func(_ []byte, resource T) error {
		return resource.validateCatalogResource()
	})
}

func (c *Client) catalogURL(
	segments []string,
	options CatalogListOptions,
) (*url.URL, error) {
	if options.PageSize < 0 || options.PageSize > 1000 {
		return nil, newError(
			ErrorKindConfigInvalid,
			"catalog PageSize must be zero or an integer from 1 to 1000",
			nil,
		)
	}
	u, err := c.resourceURL(segments...)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	if options.PageSize != 0 {
		query.Set("page_size", strconv.Itoa(options.PageSize))
	}
	if options.PageToken != "" {
		query.Set("page_token", options.PageToken)
	}
	u.RawQuery = query.Encode()
	return u, nil
}

func fetchJSON[T any](
	ctx context.Context,
	c *Client,
	u *url.URL,
	validate func([]byte, T) error,
) (T, error) {
	var zero T
	resp, err := c.http.doGet(ctx, u)
	if err != nil {
		return zero, err
	}
	defer sneakyBodyClose(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to read catalog response",
			err,
		)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return zero, responseError(resp, body, ErrorKindUnexpected)
	}

	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return zero, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to decode catalog response",
			err,
		)
	}
	if err := validate(body, value); err != nil {
		return zero, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"invalid catalog response",
			err,
		)
	}
	return value, nil
}

func validateCatalogPage[T catalogResource](body []byte, page CatalogPage[T]) error {
	trimmedBody := bytes.TrimSpace(body)
	if len(trimmedBody) == 0 || trimmedBody[0] != '{' {
		return fmt.Errorf("catalog page must be a JSON object")
	}

	var shape struct {
		Items         json.RawMessage `json:"items"`
		NextPageToken json.RawMessage `json:"next_page_token"`
	}
	if err := json.Unmarshal(body, &shape); err != nil {
		return err
	}

	if len(shape.Items) == 0 {
		return fmt.Errorf("catalog page is missing items")
	}
	if bytes.Equal(bytes.TrimSpace(shape.Items), []byte("null")) {
		return fmt.Errorf("catalog page items must be an array")
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(shape.Items, &rawItems); err != nil {
		return fmt.Errorf("catalog page items must be an array: %w", err)
	}
	for index, rawItem := range rawItems {
		if bytes.Equal(bytes.TrimSpace(rawItem), []byte("null")) {
			return fmt.Errorf("catalog page item %d must be an object", index)
		}
		if err := page.Items[index].validateCatalogResource(); err != nil {
			return fmt.Errorf("catalog page item %d is invalid: %w", index, err)
		}
	}

	if len(shape.NextPageToken) != 0 {
		if !bytes.Equal(bytes.TrimSpace(shape.NextPageToken), []byte("null")) {
			var token string
			if err := json.Unmarshal(shape.NextPageToken, &token); err != nil {
				return fmt.Errorf("catalog page next_page_token must be a string or null: %w", err)
			}
		}
	}
	return nil
}

func iterateCatalog[T any](
	options CatalogListOptions,
	fetch func(CatalogListOptions) (CatalogPage[T], error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		seenTokens := make(map[string]struct{})
		if options.PageToken != "" {
			seenTokens[options.PageToken] = struct{}{}
		}

		for {
			page, err := fetch(options)
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, item := range page.Items {
				if !yield(item, nil) {
					return
				}
			}

			if page.NextPageToken == "" {
				return
			}
			if _, exists := seenTokens[page.NextPageToken]; exists {
				var zero T
				yield(zero, newError(
					ErrorKindUnexpected,
					"catalog returned a repeated page token",
					nil,
				))
				return
			}
			seenTokens[page.NextPageToken] = struct{}{}
			options.PageToken = page.NextPageToken
		}
	}
}
