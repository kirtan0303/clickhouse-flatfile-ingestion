package main

import (
    "context"
    "encoding/csv"
    "encoding/json"
    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/gorilla/mux"
    "io"
    "log"
    "net/http"
    "os"
    "strings"
)

type Config struct {
    Source     string `json:"source"`
    Host       string `json:"host"`
    Port       string `json:"port"`
    Database   string `json:"database"`
    User       string `json:"user"`
    JWT        string `json:"jwt"`
    FileName   string `json:"fileName"`
    Delimiter  string `json:"delimiter"`
}

type Schema struct {
    Table   string   `json:"table"`
    Columns []string `json:"columns"`
}

type IngestionRequest struct {
    Source      string   `json:"source"`
    Target      string   `json:"target"`
    Table       string   `json:"table"`
    Columns     []string `json:"columns"`
    FileName    string   `json:"fileName"`
    Delimiter   string   `json:"delimiter"`
}

func main() {
    r := mux.NewRouter()
    r.HandleFunc("/connect", connect).Methods("POST")
    r.HandleFunc("/schemas", getSchemas).Methods("POST")
    r.HandleFunc("/ingest", ingestData).Methods("POST")
    log.Fatal(http.ListenAndServe(":8080", r))
}

func connect(w http.ResponseWriter, r *http.Request) {
    var cfg Config
    if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
        http.Error(w, "Invalid config", http.StatusBadRequest)
        return
    }

    if cfg.Source == "ClickHouse" {
        conn, err := clickhouse.Open(&clickhouse.Options{
            Addr: []string{cfg.Host + ":" + cfg.Port},
            Auth: clickhouse.Auth{
                Database: cfg.Database,
                Username: cfg.User,
                Password: cfg.JWT,
            },
        })
        if err != nil {
            http.Error(w, "Connection failed: "+err.Error(), http.StatusInternalServerError)
            return
        }
        defer conn.Close()
        if err := conn.Ping(context.Background()); err != nil {
            http.Error(w, "Ping failed: "+err.Error(), http.StatusInternalServerError)
            return
        }
    } else {
        // Validate Flat File existence
        if _, err := os.Stat(cfg.FileName); os.IsNotExist(err) {
            http.Error(w, "File not found", http.StatusBadRequest)
            return
        }
    }

    json.NewEncoder(w).Encode(map[string]string{"status": "connected"})
}

func getSchemas(w http.ResponseWriter, r *http.Request) {
    var cfg Config
    if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
        http.Error(w, "Invalid config", http.StatusBadRequest)
        return
    }

    var schemas []Schema
    if cfg.Source == "ClickHouse" {
        conn, err := clickhouse.Open(&clickhouse.Options{
            Addr: []string{cfg.Host + ":" + cfg.Port},
            Auth: clickhouse.Auth{
                Database: cfg.Database,
                Username: cfg.User,
                Password: cfg.JWT,
            },
        })
        if err != nil {
            http.Error(w, "Connection failed", http.StatusInternalServerError)
            return
        }
        defer conn.Close()

        rows, err := conn.Query(context.Background(), "SELECT name FROM system.tables WHERE database = ?", cfg.Database)
        if err != nil {
            http.Error(w, "Query failed", http.StatusInternalServerError)
            return
        }
        defer rows.Close()

        for rows.Next() {
            var table string
            if err := rows.Scan(&table); err != nil {
                http.Error(w, "Scan failed", http.StatusInternalServerError)
                return
            }
            cols, err := getColumns(conn, cfg.Database, table)
            if err != nil {
                http.Error(w, "Column fetch failed", http.StatusInternalServerError)
                return
            }
            schemas = append(schemas, Schema{Table: table, Columns: cols})
        }
    } else {
        file, err := os.Open(cfg.FileName)
        if err != nil {
            http.Error(w, "File open failed", http.StatusBadRequest)
            return
        }
        defer file.Close()

        reader := csv.NewReader(file)
        reader.Comma = rune(cfg.Delimiter[0])
        headers, err := reader.Read()
        if err != nil {
            http.Error(w, "CSV read failed", http.StatusBadRequest)
            return
        }
        schemas = append(schemas, Schema{Table: cfg.FileName, Columns: headers})
    }

    json.NewEncoder(w).Encode(schemas)
}

func getColumns(conn *clickhouse.Conn, db, table string) ([]string, error) {
    rows, err := conn.Query(context.Background(), "SELECT name FROM system.columns WHERE database = ? AND table = ?", db, table)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var columns []string
    for rows.Next() {
        var col string
        if err := rows.Scan(&col); err != nil {
            return nil, err
        }
        columns = append(columns, col)
    }
    return columns, nil
}

func ingestData(w http.ResponseWriter, r *http.Request) {
    var req IngestionRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request", http.StatusBadRequest)
        return
    }

    var cfg Config // Assume cfg is passed or stored from previous connect call
    var count int64

    if req.Source == "ClickHouse" {
        conn, err := clickhouse.Open(&clickhouse.Options{
            Addr: []string{cfg.Host + ":" + cfg.Port},
            Auth: clickhouse.Auth{
                Database: cfg.Database,
                Username: cfg.User,
                Password: cfg.JWT,
            },
        })
        if err != nil {
            http.Error(w, "Connection failed", http.StatusInternalServerError)
            return
        }
        defer conn.Close()

        query := "SELECT " + strings.Join(req.Columns, ", ") + " FROM " + req.Table
        rows, err := conn.Query(context.Background(), query)
        if err != nil {
            http.Error(w, "Query failed", http.StatusInternalServerError)
            return
        }
        defer rows.Close()

        file, err := os.Create(req.FileName)
        if err != nil {
            http.Error(w, "File creation failed", http.StatusInternalServerError)
            return
        }
        defer file.Close()

        writer := csv.NewWriter(file)
        writer.Comma = rune(req.Delimiter[0])
        if err := writer.Write(req.Columns); err != nil {
            http.Error(w, "CSV write failed", http.StatusInternalServerError)
            return
        }

        for rows.Next() {
            values := make([]interface{}, len(req.Columns))
            valuePtrs := make([]interface{}, len(req.Columns))
            for i := range values {
                valuePtrs[i] = &values[i]
            }
            if err := rows.Scan(valuePtrs...); err != nil {
                http.Error(w, "Scan failed", http.StatusInternalServerError)
                return
            }
            strValues := make([]string, len(values))
            for i, v := range values {
                strValues[i] = fmt.Sprintf("%v", v)
            }
            if err := writer.Write(strValues); err != nil {
                http.Error(w, "CSV write failed", http.StatusInternalServerError)
                return
            }
            count++
        }
        writer.Flush()
    } else {
        // Flat File to ClickHouse
        // Create table, read CSV, insert in batches
        // Placeholder for full implementation
        count = 0 // Update with actual count
    }

    json.NewEncoder(w).Encode(map[string]int64{"count": count})
}
