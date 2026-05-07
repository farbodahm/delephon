package bq

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const maxRows = 10000

type QueryResult struct {
	Columns        []string
	Rows           [][]string
	RowCount       int64
	Duration       time.Duration
	BytesProcessed int64
}

type TableSchema struct {
	Fields         []SchemaField
	PartitionField string // empty if not partitioned, or the column name (e.g. "_PARTITIONTIME" for ingestion-time)
	PartitionType  string // "DAY", "HOUR", "MONTH", "YEAR", or ""
}

type SchemaField struct {
	Name        string
	Type        string
	Mode        string
	Description string
}

type Client struct {
	clients map[string]*bigquery.Client
	ctx     context.Context
}

func NewManager(ctx context.Context) *Client {
	return &Client{
		clients: make(map[string]*bigquery.Client),
		ctx:     ctx,
	}
}

func (c *Client) getClient(projectID string) (*bigquery.Client, error) {
	if cl, ok := c.clients[projectID]; ok {
		return cl, nil
	}
	cl, err := NewClient(c.ctx, projectID)
	if err != nil {
		return nil, err
	}
	c.clients[projectID] = cl
	return cl, nil
}

func (c *Client) getAnyClient(fallbackProjectID string) (*bigquery.Client, error) {
	for _, cl := range c.clients {
		return cl, nil
	}
	return c.getClient(fallbackProjectID)
}

func (c *Client) Close() {
	for _, cl := range c.clients {
		cl.Close()
	}
}

func (c *Client) ListProjects(ctx context.Context) ([]string, error) {
	creds, err := FindDefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}
	svc, err := crmv1.NewService(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("resource manager: %w", err)
	}
	var projects []string
	req := svc.Projects.List().PageSize(100)
	err = req.Pages(ctx, func(page *crmv1.ListProjectsResponse) error {
		for _, p := range page.Projects {
			if p.LifecycleState == "ACTIVE" {
				projects = append(projects, p.ProjectId)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	return projects, nil
}

func (c *Client) ListDatasets(ctx context.Context, projectID string) ([]string, error) {
	cl, err := c.getAnyClient(projectID)
	if err != nil {
		return nil, err
	}
	var datasets []string
	it := cl.Datasets(ctx)
	it.ProjectID = projectID
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list datasets: %w", err)
		}
		datasets = append(datasets, ds.DatasetID)
	}
	return datasets, nil
}

func (c *Client) ListTables(ctx context.Context, projectID, datasetID string) ([]string, error) {
	cl, err := c.getAnyClient(projectID)
	if err != nil {
		return nil, err
	}
	var tables []string
	it := cl.DatasetInProject(projectID, datasetID).Tables(ctx)
	for {
		t, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		tables = append(tables, t.TableID)
	}
	return tables, nil
}

func (c *Client) GetTableSchema(ctx context.Context, projectID, datasetID, tableID string) (*TableSchema, error) {
	cl, err := c.getAnyClient(projectID)
	if err != nil {
		return nil, err
	}
	md, err := cl.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("table metadata: %w", err)
	}
	schema := &TableSchema{}
	for _, f := range md.Schema {
		mode := "NULLABLE"
		if f.Required {
			mode = "REQUIRED"
		}
		if f.Repeated {
			mode = "REPEATED"
		}
		schema.Fields = append(schema.Fields, SchemaField{
			Name:        f.Name,
			Type:        string(f.Type),
			Mode:        mode,
			Description: f.Description,
		})
	}

	// Extract partitioning info
	if tp := md.TimePartitioning; tp != nil {
		if tp.Field != "" {
			schema.PartitionField = tp.Field
		} else {
			schema.PartitionField = "_PARTITIONTIME"
		}
		switch tp.Type {
		case bigquery.DayPartitioningType:
			schema.PartitionType = "DAY"
		case bigquery.HourPartitioningType:
			schema.PartitionType = "HOUR"
		case bigquery.MonthPartitioningType:
			schema.PartitionType = "MONTH"
		case bigquery.YearPartitioningType:
			schema.PartitionType = "YEAR"
		}
	} else if rp := md.RangePartitioning; rp != nil {
		schema.PartitionField = rp.Field
		schema.PartitionType = "RANGE"
	}

	return schema, nil
}

func (c *Client) RunQuery(ctx context.Context, projectID, sqlText string) (*QueryResult, error) {
	result := &QueryResult{}
	err := c.runQueryInternal(ctx, projectID, sqlText, result, nil, nil)
	return result, err
}

// StreamingResult contains query metadata returned by RunQueryStreaming.
type StreamingResult struct {
	Columns        []string
	Duration       time.Duration
	BytesProcessed int64
	RowCount       int64
}

// RunQueryStreaming executes a query and delivers results incrementally.
// onColumns is called once the schema is known (before any rows).
// onBatch is called with each batch of rows as they arrive from BigQuery.
func (c *Client) RunQueryStreaming(ctx context.Context, projectID, sqlText string,
	onColumns func(columns []string),
	onBatch func(rows [][]string),
) (*StreamingResult, error) {
	sr := &StreamingResult{}
	qr := &QueryResult{}
	err := c.runQueryInternal(ctx, projectID, sqlText, qr, onColumns, onBatch)
	if err != nil {
		return nil, err
	}
	sr.Columns = qr.Columns
	sr.Duration = qr.Duration
	sr.BytesProcessed = qr.BytesProcessed
	sr.RowCount = qr.RowCount
	return sr, nil
}

// runQueryInternal is the shared implementation for RunQuery and RunQueryStreaming.
// When onColumns/onBatch are nil, rows are collected into qr.Rows (batch mode).
// When non-nil, rows are delivered via callbacks (streaming mode).
func (c *Client) runQueryInternal(ctx context.Context, projectID, sqlText string,
	qr *QueryResult,
	onColumns func([]string),
	onBatch func([][]string),
) error {
	cl, err := c.getClient(projectID)
	if err != nil {
		return err
	}

	start := time.Now()
	q := cl.Query(sqlText)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait query: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("query error: %w", status.Err())
	}

	qr.Duration = time.Since(start)

	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("read results: %w", err)
	}

	if status.Statistics != nil {
		qr.BytesProcessed = status.Statistics.TotalBytesProcessed
	}

	// Extract column names from schema.
	if it.Schema != nil {
		for _, f := range it.Schema {
			qr.Columns = append(qr.Columns, f.Name)
		}
	}

	// Deliver columns immediately so the UI can show headers.
	if onColumns != nil {
		onColumns(qr.Columns)
	}

	// Read rows.
	const streamBatchSize = 100
	var batch [][]string
	if onBatch != nil {
		batch = make([][]string, 0, streamBatchSize)
	}
	firstRow := true

	for qr.RowCount < int64(maxRows) {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			// Flush any pending batch before returning the error.
			if onBatch != nil && len(batch) > 0 {
				onBatch(batch)
			}
			return fmt.Errorf("read row: %w", err)
		}
		strRow := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				strRow[i] = "NULL"
			} else {
				strRow[i] = fmt.Sprintf("%v", v)
			}
		}
		qr.RowCount++

		if onBatch != nil {
			batch = append(batch, strRow)
			// Flush immediately on first row for instant feedback, then in batches.
			if firstRow || len(batch) >= streamBatchSize {
				onBatch(batch)
				batch = make([][]string, 0, streamBatchSize)
				firstRow = false
			}
		} else {
			qr.Rows = append(qr.Rows, strRow)
		}
	}

	// Flush remaining rows.
	if onBatch != nil && len(batch) > 0 {
		onBatch(batch)
	}

	return nil
}
