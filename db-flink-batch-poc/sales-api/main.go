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

type OrderEvent struct {
	OrderID    string  `json:"orderId"`
	SellerID   string  `json:"sellerId"`
	SellerName string  `json:"sellerName"`
	Location   string  `json:"location"`
	ProductID  string  `json:"productId"`
	Quantity   int     `json:"quantity"`
	TotalPrice float64 `json:"totalPrice"`
	OrderDate  string  `json:"orderDate"`
	Channel    string  `json:"channel"`
	Source     string  `json:"source"`
}

type seller struct {
	id   string
	name string
}

var locations = []string{
	"New York", "Boston", "Philadelphia", "Washington DC",
	"Los Angeles", "San Francisco", "Seattle", "San Diego",
	"Chicago", "Detroit", "Minneapolis",
	"Houston", "Atlanta", "Dallas", "Miami", "San Antonio",
	"Phoenix", "Denver", "Las Vegas",
}

var channels = []string{"online", "in-store", "phone"}

var (
	mu      sync.Mutex
	events  []OrderEvent
	seq     int
	sellers []seller
	products []string
)

func buildPools(n int) {
	sellers = make([]seller, n)
	for i := range sellers {
		sellers[i] = seller{
			id:   fmt.Sprintf("SL%03d", i+1),
			name: gofakeit.Name(),
		}
	}
	log.Printf("[sales-api] seller pool (%d):", n)
	for _, s := range sellers {
		log.Printf("  %s  %s", s.id, s.name)
	}

	products = make([]string, 8)
	for i := range products {
		products[i] = fmt.Sprintf("P%03d", i+1)
	}
}

func generateEvent() OrderEvent {
	mu.Lock()
	seq++
	id := seq
	mu.Unlock()

	loc := locations[gofakeit.Number(0, len(locations)-1)]
	sl := sellers[gofakeit.Number(0, len(sellers)-1)]
	product := products[gofakeit.Number(0, len(products)-1)]
	channel := channels[gofakeit.Number(0, len(channels)-1)]

	return OrderEvent{
		OrderID:    fmt.Sprintf("API%07d", id),
		SellerID:   sl.id,
		SellerName: sl.name,
		Location:   loc,
		ProductID:  product,
		Quantity:   gofakeit.Number(1, 20),
		TotalPrice: gofakeit.Price(100, 5000),
		OrderDate:  time.Now().UTC().Format(time.RFC3339),
		Channel:    channel,
		Source:     "api",
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
	}
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	snapshot := make([]OrderEvent, len(events))
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
	sellersCount := envOrDefaultInt("SELLERS_COUNT", 8)
	fakerSeed := envOrDefaultInt64("FAKER_SEED", 0)

	if fakerSeed != 0 {
		gofakeit.Seed(fakerSeed)
		log.Printf("[sales-api] faker seed=%d (reproducible)", fakerSeed)
	}

	buildPools(sellersCount)
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
