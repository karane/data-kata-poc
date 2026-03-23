package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	port := envOrDefault("PORT", "8080")

	hostDB := envOrDefault("DB_HOST", "postgres-sink")
	portDB := envOrDefault("DB_PORT", "5432")
	userDB := envOrDefault("DB_USER", "poc")
	passwordDB := envOrDefault("DB_PASSWORD", "poc123")
	dbname := envOrDefault("DB_NAME", "salesdb")
	sslmode := envOrDefault("DB_SSLMODE", "disable")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		userDB, passwordDB, hostDB, portDB, dbname, sslmode,
	)

    log.Printf("[aggregate-api] starting port=%s dbHost=%s dbPort=%s dbUser=%s dbName=%s", port, hostDB, portDB, userDB, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("[aggregate-api] env=dbConnectionOpen connStr=%s fatal: %v", connStr, err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal("[aggregate-api] env=dbPing DB not ready: %v", err)
	}

	store := &PostgresStore{DB: db}
	handler := NewHandler(store)

	http.HandleFunc("/health", handler.Health)
	http.HandleFunc("/top-sales-by-city", handler.TopSalesByCity)
	http.HandleFunc("/top-salesman", handler.TopSalesman)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[aggregate-api] env=appListenAndServe fatal: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
