package bq

import (
	"context"
	"fmt"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"cloud.google.com/go/bigquery"
)

func FindDefaultCredentials(ctx context.Context) (*google.Credentials, error) {
	creds, err := google.FindDefaultCredentials(ctx,
		bigquery.Scope,
		"https://www.googleapis.com/auth/cloud-platform.read-only",
	)
	if err != nil {
		return nil, fmt.Errorf("ADC not found (run 'gcloud auth application-default login'): %w", err)
	}
	return creds, nil
}

func NewClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	creds, err := FindDefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("bigquery client: %w", err)
	}
	return client, nil
}
