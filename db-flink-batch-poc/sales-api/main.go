package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type SaleEvent struct {
	SaleID       string  `json:"saleId"`
	SalesmanID   string  `json:"salesmanId"`
	SalesmanName string  `json:"salesmanName"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	ProductID    string  `json:"productId"`
	Amount       float64 `json:"amount"`
	EventTime    int64   `json:"eventTime"`
	Source       string  `json:"source"`
}

type salesman struct {
	id   string
	name string
}

var cityRegions = [][2]string{
	{"New York", "Northeast"}, {"Boston", "Northeast"}, {"Philadelphia", "Northeast"},
	{"Washington DC", "Northeast"}, {"Los Angeles", "West"}, {"San Francisco", "West"},
	{"Seattle", "West"}, {"San Diego", "West"}, {"Chicago", "Midwest"},
	{"Detroit", "Midwest"}, {"Minneapolis", "Midwest"}, {"Houston", "South"},
	{"Atlanta", "South"}, {"Dallas", "South"}, {"Miami", "South"},
	{"San Antonio", "South"}, {"Phoenix", "Southwest"}, {"Denver", "Southwest"},
	{"Las Vegas", "Southwest"},
}

var (
	mu       sync.Mutex
	events   []SaleEvent
	seq      int
	salesmen []salesman
	products []string
)

func buildPools(n int) {
	salesmen = make([]salesman, n)
	for i := range salesmen {
		salesmen[i] = salesman{
			id:   fmt.Sprintf("SM%03d", i+1),
			name: gofakeit.Name(),
		}
	}
	log.Printf("[sales-api] salesman pool (%d):", n)
	for _, s := range salesmen {
		log.Printf("  %s  %s", s.id, s.name)
	}

	products = make([]string, 8)
	for i := range products {
		products[i] = fmt.Sprintf("P%03d", i+1)
	}
}

func generateEvent() SaleEvent {
	mu.Lock()
	seq++
	id := seq
	mu.Unlock()

	cr := cityRegions[gofakeit.Number(0, len(cityRegions)-1)]
	sm := salesmen[gofakeit.Number(0, len(salesmen)-1)]
	product := products[gofakeit.Number(0, len(products)-1)]

	return SaleEvent{
		SaleID:       fmt.Sprintf("API%07d", id),
		SalesmanID:   sm.id,
		SalesmanName: sm.name,
		City:         cr[0],
		Region:       cr[1],
		ProductID:    product,
		Amount:       gofakeit.Price(100, 5000),
		EventTime:    time.Now().UnixMilli(),
		Source:       "api",
	}
}

func seedLoop(intervalMs int) {
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		e := generateEvent()
		mu.Lock()
		events = append(events, e)
		if len(events) > 200 { // keep last 200
			events = events[len(events)-200:]
		}
		mu.Unlock()
		log.Printf("[seed] generated %s  city=%s  amount=%.2f", e.SaleID, e.City, e.Amount)
	}
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	snapshot := make([]SaleEvent, len(events))
	copy(snapshot, events)
	events = events[:0] // drain after each poll
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(snapshot); err != nil {
		log.Printf("[api] encode error: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

func main() {
	port := envOrDefault("PORT", "8080")
	intervalMs := envOrDefaultInt("SEED_INTERVAL_MS", 4000)
	salesmenCount := envOrDefaultInt("SALESMEN_COUNT", 8)
	fakerSeed := envOrDefaultInt64("FAKER_SEED", 0)

	if fakerSeed != 0 {
		gofakeit.Seed(fakerSeed)
		log.Printf("[sales-api] faker seed=%d (reproducible)", fakerSeed)
	}

	buildPools(salesmenCount)
	log.Printf("[sales-api] starting  port=%s  seedInterval=%dms", port, intervalMs)

	go seedLoop(intervalMs)

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/sales/events", handleEvents)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[sales-api] fatal: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envOrDefaultInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
