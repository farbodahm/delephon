//go:build integration

package bq

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/testcontainers/testcontainers-go"
	tcbigquery "github.com/testcontainers/testcontainers-go/modules/gcloud/bigquery"
)

const projectID = "test-project"
const otherProjectID = "other-project"

var dataYAML = []byte(`
projects:
- id: test-project
  datasets:
  - id: test_dataset
    tables:
    - id: users
      columns:
      - name: id
        type: INTEGER
      - name: name
        type: STRING
      - name: email
        type: STRING
      data:
      - id: 1
        name: Alice
        email: alice@example.com
      - id: 2
        name: Bob
        email: bob@example.com
      - id: 3
        name: Charlie
        email: charlie@example.com
- id: other-project
  datasets:
  - id: other_dataset
    tables:
    - id: orders
      columns:
      - name: order_id
        type: INTEGER
      - name: product
        type: STRING
      data:
      - order_id: 100
        product: Widget
`)

var testClient *Client

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcbigquery.Run(
		ctx,
		"ghcr.io/goccy/bigquery-emulator:0.6.1",
		tcbigquery.WithProjectID(projectID),
		tcbigquery.WithDataYAML(bytes.NewReader(dataYAML)),
		testcontainers.WithImagePlatform("linux/amd64"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start bigquery emulator: %v\n", err)
		os.Exit(1)
	}

	opts := []option.ClientOption{
		option.WithEndpoint(container.URI()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}

	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create bigquery client: %v\n", err)
		os.Exit(1)
	}

	testClient = NewManager(ctx)
	testClient.clients[projectID] = bqClient

	code := m.Run()

	bqClient.Close()
	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate container: %v\n", err)
	}

	os.Exit(code)
}

func TestListDatasets(t *testing.T) {
	datasets, err := testClient.ListDatasets(context.Background(), projectID)
	if err != nil {
		t.Fatalf("ListDatasets: %v", err)
	}

	if !slices.Contains(datasets, "test_dataset") {
		t.Errorf("expected test_dataset in datasets, got %v", datasets)
	}
}

func TestListTables(t *testing.T) {
	tables, err := testClient.ListTables(context.Background(), projectID, "test_dataset")
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}

	if !slices.Contains(tables, "users") {
		t.Errorf("expected users in tables, got %v", tables)
	}
}

func TestGetTableSchema(t *testing.T) {
	schema, err := testClient.GetTableSchema(context.Background(), projectID, "test_dataset", "users")
	if err != nil {
		t.Fatalf("GetTableSchema: %v", err)
	}

	wantFields := map[string]string{
		"id":    "INTEGER",
		"name":  "STRING",
		"email": "STRING",
	}

	if len(schema.Fields) != len(wantFields) {
		t.Fatalf("expected %d fields, got %d", len(wantFields), len(schema.Fields))
	}

	for _, f := range schema.Fields {
		wantType, ok := wantFields[f.Name]
		if !ok {
			t.Errorf("unexpected field %q", f.Name)
			continue
		}
		if f.Type != wantType {
			t.Errorf("field %q: expected type %q, got %q", f.Name, wantType, f.Type)
		}
	}
}

func TestRunQuery(t *testing.T) {
	result, err := testClient.RunQuery(context.Background(), projectID, "SELECT 1 AS num, 'hello' AS greeting")
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "num" || result.Columns[1] != "greeting" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}

	if result.RowCount != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount)
	}
	if result.Rows[0][0] != "1" || result.Rows[0][1] != "hello" {
		t.Errorf("unexpected row values: %v", result.Rows[0])
	}
}

func TestRunQueryFromTable(t *testing.T) {
	result, err := testClient.RunQuery(context.Background(), projectID, "SELECT * FROM test_dataset.users ORDER BY id")
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if result.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", result.RowCount)
	}

	// Verify all seeded rows are returned
	wantNames := []string{"Alice", "Bob", "Charlie"}
	nameIdx := slices.Index(result.Columns, "name")
	if nameIdx == -1 {
		t.Fatal("name column not found in results")
	}

	for i, wantName := range wantNames {
		if result.Rows[i][nameIdx] != wantName {
			t.Errorf("row %d: expected name %q, got %q", i, wantName, result.Rows[i][nameIdx])
		}
	}
}

// TestCrossProjectBrowsing verifies that a client created for test-project can
// browse datasets, tables, and schemas in other-project via the cross-project
// SDK methods (DatasetsInProject / DatasetInProject).
func TestCrossProjectBrowsing(t *testing.T) {
	// The testClient only has a *bigquery.Client keyed to "test-project".
	// getAnyClient will reuse that client for browsing other-project.

	datasets, err := testClient.ListDatasets(context.Background(), otherProjectID)
	if err != nil {
		t.Fatalf("ListDatasets(other-project): %v", err)
	}
	if !slices.Contains(datasets, "other_dataset") {
		t.Errorf("expected other_dataset in datasets, got %v", datasets)
	}

	tables, err := testClient.ListTables(context.Background(), otherProjectID, "other_dataset")
	if err != nil {
		t.Fatalf("ListTables(other-project, other_dataset): %v", err)
	}
	if !slices.Contains(tables, "orders") {
		t.Errorf("expected orders in tables, got %v", tables)
	}

	schema, err := testClient.GetTableSchema(context.Background(), otherProjectID, "other_dataset", "orders")
	if err != nil {
		t.Fatalf("GetTableSchema(other-project, other_dataset, orders): %v", err)
	}
	wantFields := map[string]string{
		"order_id": "INTEGER",
		"product":  "STRING",
	}
	if len(schema.Fields) != len(wantFields) {
		t.Fatalf("expected %d fields, got %d", len(wantFields), len(schema.Fields))
	}
	for _, f := range schema.Fields {
		wantType, ok := wantFields[f.Name]
		if !ok {
			t.Errorf("unexpected field %q", f.Name)
			continue
		}
		if f.Type != wantType {
			t.Errorf("field %q: expected type %q, got %q", f.Name, wantType, f.Type)
		}
	}
}
