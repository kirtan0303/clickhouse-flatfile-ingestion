package main

import (
    "context"
    "github.com/ClickHouse/clickhouse-go/v2"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
)

func TestConnectClickHouse(t *testing.T) {
    req, err := http.NewRequest("POST", "/connect", strings.NewReader(`{
        "source": "ClickHouse",
        "host": "localhost",
        "port": "9000",
        "database": "default",
        "user": "default",
        "jwt": ""
    }`))
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    handler := http.HandlerFunc(connect)
    handler.ServeHTTP(rr, req)

    if status := rr.Code; status != http.StatusOK {
        t.Errorf("Expected status %v, got %v", http.StatusOK, status)
    }
    expected := `{"status":"connected"}`
    if rr.Body.String() != expected {
        t.Errorf("Expected body %v, got %v", expected, rr.Body.String())
    }
}

func TestIngestClickHouseToFlatFile(t *testing.T) {
    // Setup ClickHouse connection and load uk_price_paid dataset
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{Database: "default", Username: "default", Password: ""},
    })
    if err != nil {
        t.Fatal(err)
    }
    defer conn.Close()

    // Create test table
    err = conn.Exec(context.Background(), `
        CREATE TABLE test_table (id UInt32, price UInt32) ENGINE = Memory
        INSERT INTO test_table VALUES (1, 100), (2, 200)
    `)
    if err != nil {
        t.Fatal(err)
    }

    req, err := http.NewRequest("POST", "/ingest", strings.NewReader(`{
        "source": "ClickHouse",
        "target": "Flat File",
        "table": "test_table",
        "columns": ["id", "price"],
        "fileName": "test_output.csv",
        "delimiter": ","
    }`))
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    handler := http.HandlerFunc(ingestData)
    handler.ServeHTTP(rr, req)

    if status := rr.Code; status != http.StatusOK {
        t.Errorf("Expected status %v, got %v", http.StatusOK, status)
    }
    expected := `{"count":2}`
    if rr.Body.String() != expected {
        t.Errorf("Expected body %v, got %v", expected, rr.Body.String())
    }

    // Verify CSV content
    // Additional verification code would be added
}
