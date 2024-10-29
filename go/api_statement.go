package scopedb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"io"
	"net/url"
	"time"
)

type statementAPI interface {
	// submitStatement submits a statement to the ScopeDB server and returns the statement ID.
	submitStatement(ctx context.Context, req *StatementRequest) (*StatementResponse, error)
	// fetchStatementResult fetches the result of a statement by its ID.
	fetchStatementResult(ctx context.Context, params *FetchStatementParams) (*StatementResponse, error)
}

var _ statementAPI = (*Connection)(nil)

type StatementRequest struct {
	// Statement is the ScopeQL statement to execute.
	Statement string `json:"statement"`
	// WaitTimeout is the maximum time to wait for the statement to finish.
	// Possible values like "60s".
	WaitTimeout string `json:"wait_timeout"`
	// Format is the format of the result set.
	Format ResultFormat `json:"format"`
}

type FetchStatementParams struct {
	// StatementId is the ID of the statement to fetch.
	StatementId string
	// Format is the format of the result set.
	Format ResultFormat
}

type ResultFormat string

const (
	// ArrowJSONFormat parses the result set as Arrow format with variant rendered as JSON (BASE64 encoded).
	ArrowJSONFormat ResultFormat = "arrow-json"
)

type QueryStatus string

const (
	QueryStatusStarted  QueryStatus = "started"
	QueryStatusRunning  QueryStatus = "running"
	QueryStatusFinished QueryStatus = "finished"
)

type StatementResponse struct {
	StatementId string      `json:"statement_id"`
	Status      QueryStatus `json:"status"`
	ResultSet   *ResultSet  `json:"result_set"`
}

type ResultSet struct {
	Metadata *ResultSetMetadata `json:"metadata"`
	Rows     string             `json:"rows"`
}

type ResultSetMetadata struct {
	Fields []*ResultSetField `json:"fields"`
}

type ResultSetField struct {
	Name     string `json:"name"`
	DataType string `json:"data_Type"`
}

type ArrowResultSet struct {
	StatementId string
	Metadata    *ResultSetMetadata
	Records     *[]arrow.Record
}

func (rs *StatementResponse) ToArrowResultSet() (*ArrowResultSet, error) {
	if rs.ResultSet == nil {
		return nil, errors.New("result set is not available")
	}

	records, err := decodeRecordBatches([]byte(rs.ResultSet.Rows))
	if err != nil {
		return nil, err
	}
	return &ArrowResultSet{
		StatementId: rs.StatementId,
		Metadata:    rs.ResultSet.Metadata,
		Records:     &records,
	}, nil
}

type ResultSetFetcher struct {
	conn        *Connection
	fetchParams *FetchStatementParams
}

func NewResultSetFetcher(conn *Connection, params *FetchStatementParams) *ResultSetFetcher {
	return &ResultSetFetcher{
		conn:        conn,
		fetchParams: params,
	}
}

const defaultFetchTimeout = 60 * time.Second
const defaultFetchInterval = 1 * time.Second

func (f *ResultSetFetcher) FetchResultSet(ctx context.Context) (*StatementResponse, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultFetchTimeout)
	}

	ticker := time.NewTicker(defaultFetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				msg := fmt.Sprintf("fetch result set timeout after after %v: %s", time.Since(deadline), f.fetchParams.StatementId)
				return nil, errors.New(msg)
			}

			resp, err := f.conn.fetchStatementResult(ctx, f.fetchParams)
			if err != nil {
				return nil, err
			}
			if resp.Status != QueryStatusFinished {
				continue
			}
			return resp, nil
		}
	}
}

func (conn *Connection) submitStatement(ctx context.Context, req *StatementRequest) (*StatementResponse, error) {
	url, err := url.Parse(conn.config.Endpoint + "/v1/statements")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := conn.http.Post(ctx, url, body)
	if err != nil {
		return nil, err
	}
	if err := checkStatusCodeOK(resp.StatusCode); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData StatementResponse
	err = json.Unmarshal(data, &respData)
	return &respData, err
}

func (conn *Connection) fetchStatementResult(ctx context.Context, fetchParams *FetchStatementParams) (*StatementResponse, error) {
	url, err := url.Parse(conn.config.Endpoint + "/v1/statements/" + fetchParams.StatementId)
	if err != nil {
		return nil, err
	}
	q := url.Query()
	q.Add("format", string(fetchParams.Format))
	url.RawQuery = q.Encode()

	resp, err := conn.http.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := checkStatusCodeOK(resp.StatusCode); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respData StatementResponse
	err = json.Unmarshal(data, &respData)
	return &respData, err
}
