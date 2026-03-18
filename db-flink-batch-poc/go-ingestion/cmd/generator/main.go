package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

// SaleEventFromFile represents a sale record for file output.
type SaleEventFromFile struct {
	SaleID       string  `json:"sale_id"`
	SalesmanID   string  `json:"salesman_id"`
	SalesmanName string  `json:"salesman_name"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	ProductID    string  `json:"product_id"`
	Amount       float64 `json:"amount"`
	EventTime    int64   `json:"event_time"`
}

// Predefined data for realistic generation
var (
	regions = []string{"Northeast", "West", "Midwest", "South", "Southwest"}

	citiesByRegion = map[string][]string{
		"Northeast": {"New York", "Boston", "Philadelphia", "Pittsburgh", "Hartford"},
		"West":      {"Los Angeles", "San Francisco", "Seattle", "Portland", "Denver"},
		"Midwest":   {"Chicago", "Detroit", "Minneapolis", "Cleveland", "Indianapolis"},
		"South":     {"Houston", "Dallas", "Miami", "Atlanta", "Charlotte"},
		"Southwest": {"Phoenix", "Las Vegas", "San Diego", "Albuquerque", "Tucson"},
	}

	productCategories = []string{"P001", "P002", "P003", "P004", "P005"}
)

// Salesman represents a salesperson with consistent data.
type Salesman struct {
	ID   string
	Name string
}

func main() {
	// Command line flags
	count := flag.Int("count", 100, "Number of sales records to generate")
	output := flag.String("output", "", "Output file path (default: stdout)")
	seed := flag.Int64("seed", 0, "Random seed (0 = random)")
	salesmenCount := flag.Int("salesmen", 10, "Number of unique salespeople")
	startDate := flag.String("start-date", "2024-01-01", "Start date for events (YYYY-MM-DD)")
	endDate := flag.String("end-date", "2024-12-31", "End date for events (YYYY-MM-DD)")
	minAmount := flag.Float64("min-amount", 100.0, "Minimum sale amount")
	maxAmount := flag.Float64("max-amount", 10000.0, "Maximum sale amount")
	pretty := flag.Bool("pretty", false, "Pretty print JSON output")

	flag.Parse()

	// Initialize faker
	if *seed != 0 {
		gofakeit.Seed(*seed)
	} else {
		gofakeit.Seed(time.Now().UnixNano())
	}

	// Parse dates
	start, err := time.Parse("2006-01-02", *startDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid start date: %v\n", err)
		os.Exit(1)
	}
	end, err := time.Parse("2006-01-02", *endDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid end date: %v\n", err)
		os.Exit(1)
	}

	// Generate salespeople
	salesmen := generateSalesmen(*salesmenCount)

	// Generate sales
	sales := make([]SaleEventFromFile, *count)
	for i := 0; i < *count; i++ {
		sales[i] = generateSale(i+1, salesmen, start, end, *minAmount, *maxAmount)
	}

	// Marshal to JSON
	var jsonData []byte
	if *pretty {
		jsonData, err = json.MarshalIndent(sales, "", "  ")
	} else {
		jsonData, err = json.Marshal(sales)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	// Output
	if *output != "" {
		if err := os.WriteFile(*output, jsonData, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Generated %d sales records to %s\n", *count, *output)
	} else {
		fmt.Println(string(jsonData))
	}
}

func generateSalesmen(count int) []Salesman {
	salesmen := make([]Salesman, count)
	for i := 0; i < count; i++ {
		salesmen[i] = Salesman{
			ID:   fmt.Sprintf("SM%03d", i+1),
			Name: gofakeit.Name(),
		}
	}
	return salesmen
}

func generateSale(index int, salesmen []Salesman, start, end time.Time, minAmount, maxAmount float64) SaleEventFromFile {
	// Pick random salesman
	salesman := salesmen[gofakeit.Number(0, len(salesmen)-1)]

	// Pick random region and city
	region := regions[gofakeit.Number(0, len(regions)-1)]
	cities := citiesByRegion[region]
	city := cities[gofakeit.Number(0, len(cities)-1)]

	// Generate random timestamp within range
	duration := end.Sub(start)
	randomDuration := time.Duration(gofakeit.Int64() % int64(duration))
	if randomDuration < 0 {
		randomDuration = -randomDuration
	}
	eventTime := start.Add(randomDuration)

	// Generate amount with 2 decimal places
	amount := gofakeit.Float64Range(minAmount, maxAmount)
	amount = float64(int(amount*100)) / 100

	return SaleEventFromFile{
		SaleID:       fmt.Sprintf("FS%06d", index),
		SalesmanID:   salesman.ID,
		SalesmanName: salesman.Name,
		City:         city,
		Region:       region,
		ProductID:    productCategories[gofakeit.Number(0, len(productCategories)-1)],
		Amount:       amount,
		EventTime:    eventTime.UnixMilli(),
	}
}
