package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

type SaleEvent struct {
	SaleID       string  `json:"sale_id"`
	SalesmanID   string  `json:"salesman_id"`
	SalesmanName string  `json:"salesman_name"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	ProductID    string  `json:"product_id"`
	Amount       float64 `json:"amount"`
	EventTime    int64   `json:"event_time"`
}

var regions = []string{"Northeast", "West", "Midwest", "South", "Southwest"}

var cities = map[string][]string{
	"Northeast": {"New York", "Boston", "Philadelphia"},
	"West":      {"Los Angeles", "San Francisco", "Seattle"},
	"Midwest":   {"Chicago", "Detroit", "Minneapolis"},
	"South":     {"Houston", "Miami", "Atlanta"},
	"Southwest": {"Phoenix", "Las Vegas", "Denver"},
}

var products = []string{"P001", "P002", "P003", "P004", "P005"}

func main() {
	count := flag.Int("count", 100, "Number of sales to generate")
	output := flag.String("output", "", "Output file (default: stdout)")
	pretty := flag.Bool("pretty", false, "Pretty print JSON")
	flag.Parse()

	gofakeit.Seed(time.Now().UnixNano())

	salespeople := make([]struct{ id, name string }, 10)
	for i := 0; i < 10; i++ {
		salespeople[i] = struct{ id, name string }{
			id:   fmt.Sprintf("SM%03d", i+1),
			name: gofakeit.Name(),
		}
	}

	sales := make([]SaleEvent, *count)
	for i := 0; i < *count; i++ {
		sp := salespeople[rand.Intn(len(salespeople))]
		region := regions[rand.Intn(len(regions))]
		cityList := cities[region]
		city := cityList[rand.Intn(len(cityList))]

		randomDay := rand.Intn(365)
		eventTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(randomDay) * 24 * time.Hour).
			UnixMilli()

		amount := float64(rand.Intn(9900)+100) + float64(rand.Intn(100))/100

		sales[i] = SaleEvent{
			SaleID:       fmt.Sprintf("FS%06d", i+1),
			SalesmanID:   sp.id,
			SalesmanName: sp.name,
			City:         city,
			Region:       region,
			ProductID:    products[rand.Intn(len(products))],
			Amount:       amount,
			EventTime:    eventTime,
		}
	}

	var jsonData []byte
	var err error
	if *pretty {
		jsonData, err = json.MarshalIndent(sales, "", "  ")
	} else {
		jsonData, err = json.Marshal(sales)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *output != "" {
		os.WriteFile(*output, jsonData, 0644)
		fmt.Printf("Generated %d sales to %s\n", *count, *output)
	} else {
		fmt.Println(string(jsonData))
	}
}
