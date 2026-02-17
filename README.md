# Delephon

A native desktop BigQuery client built with Go and Fyne.

<img src="screenshots/delephon-2.png" alt="Query editor with results and schema" width="800">

<img src="screenshots/delephon-1.png" alt="AI Assistant generating SQL" width="800">

## Where the idea came from

Recently at work, I found myself needing to run ad-hoc queries against BigQuery for data exploration and debugging.
The web UI is fine for quick queries, but it can be slow to load and navigate, especially when switching between projects or tables.
I wanted a lightweight, native app that would let me quickly browse my BigQuery projects, inspect schemas, and run queries without the overhead of a browser.

## Features

- **Smart project explorer** — favorites and recently queried projects show up instantly, all projects load on demand
- **AI Assistant** — describe what you want to query in plain English, and Claude generates BigQuery SQL using your table schemas as context
- **Search projects & tables** — find any table across your starred and recent projects; matching tables surface the project to the top
- **Background caching** — datasets and tables load in parallel behind the scenes, so the second search is instant
- **Query editor** — multi-tab SQL editor with Cmd+Enter / Ctrl+Enter to run
- **Context-aware autocomplete** — type `project.` to see datasets, `project.dataset.` to see tables; data loads automatically in the background when needed
- **SQL autocomplete** — SQL keywords, function names, project/dataset/table names, and column names complete as you type
- **Auto-generated queries** — click a table to get a `SELECT *` with partition filter pre-filled
- **Schema viewer** — inspect table columns, types, and descriptions
- **Query history** — browse and re-run past queries
- **Saved favorites** — bookmark queries you use often
- **Star projects** — pin frequently used projects to the top

## Install

Requires Go 1.24+ and [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

```bash
# Authenticate with GCP
gcloud auth application-default login

# Install and run
go install github.com/farbodahm/delephon@latest
delephon
```

Or clone and run locally:

```bash
git clone https://github.com/farbodahm/delephon.git
cd delephon
go run .
```
