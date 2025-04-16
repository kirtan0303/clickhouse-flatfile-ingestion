# clickhouse-flatfile-ingestion


## Overview
A web-based application for bidirectional data ingestion between ClickHouse and Flat File, with a React frontend and Go backend.

## Setup
1. Clone the repository.
2. Install Go and Node.js (optional for static server).
3. Start ClickHouse via Docker:

docker run -d -p 9000:9000 clickhouse/clickhouse-server

4. Load example datasets (e.g., `uk_price_paid`):

curl https://clickhouse.com/docs/en/getting-started/example-datasets/uk_price_paid.sql | clickhouse-client

5. Run Go backend:

go run main.go

6. Serve frontend:
- Open `index.html` in a browser, or
- Use a static server (e.g., `npx serve .`).

## Usage
1. Select source (ClickHouse or Flat File).
2. Enter connection details (e.g., Host: localhost, Port: 9000 for ClickHouse).
3. Click "Connect" to validate.
4. Click "Load Schemas" to fetch tables/columns.
5. Select a table and columns.
6. Click "Start Ingestion" to transfer data.
7. View the record count in the result section.

## Tests
- Backend tests in `main_test.go` cover connection and ingestion.
- Manual UI tests verify source selection, schema loading, and ingestion.

## Notes
- JWT is passed as the password for ClickHouse authentication.
- Flat File to ClickHouse ingestion requires additional table creation logic (placeholder included).
- Bonus features (multi-table joins, progress bar) can be added as extensions.
