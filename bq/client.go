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
	Columns  []string
	Rows     [][]string
	RowCount int64
	Duration time.Duration
	BytesProcessed int64
}

type TableSchema struct {
	Fields []SchemaField
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
	cl, err := c.getClient(projectID)
	if err != nil {
		return nil, err
	}
	var datasets []string
	it := cl.Datasets(ctx)
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
	cl, err := c.getClient(projectID)
	if err != nil {
		return nil, err
	}
	var tables []string
	it := cl.Dataset(datasetID).Tables(ctx)
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
	cl, err := c.getClient(projectID)
	if err != nil {
		return nil, err
	}
	md, err := cl.Dataset(datasetID).Table(tableID).Metadata(ctx)
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
	return schema, nil
}

func (c *Client) RunQuery(ctx context.Context, projectID, sqlText string) (*QueryResult, error) {
	cl, err := c.getClient(projectID)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	q := cl.Query(sqlText)
	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("wait query: %w", err)
	}
	if status.Err() != nil {
		return nil, fmt.Errorf("query error: %w", status.Err())
	}

	dur := time.Since(start)

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("read results: %w", err)
	}

	result := &QueryResult{
		Duration: dur,
	}
	if status.Statistics != nil {
		result.BytesProcessed = status.Statistics.TotalBytesProcessed
	}

	// Extract column names from schema
	if it.Schema != nil {
		for _, f := range it.Schema {
			result.Columns = append(result.Columns, f.Name)
		}
	}

	// Read rows
	for result.RowCount < maxRows {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		strRow := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				strRow[i] = "NULL"
			} else {
				strRow[i] = fmt.Sprintf("%v", v)
			}
		}
		result.Rows = append(result.Rows, strRow)
		result.RowCount++
	}

	return result, nil
}
