package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
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

type Product struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Price       float64   `json:"price"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Database struct {
	sales    []SaleEvent
	products map[string]Product
	mu       sync.RWMutex
}

type Config struct {
	Port       string
	WatchDir   string
	ProcessDir string
	FailedDir  string
}

var db = &Database{
	sales:    []SaleEvent{},
	products: make(map[string]Product),
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func loadConfig() Config {
	return Config{
		Port:       getEnv("PORT", "8080"),
		WatchDir:   getEnv("WATCH_DIR", "./data/inbox"),
		ProcessDir: getEnv("PROCESSED_DIR", "./data/processed"),
		FailedDir:  getEnv("FAILED_DIR", "./data/failed"),
	}
}

func main() {
	config := loadConfig()

	os.MkdirAll(config.WatchDir, 0755)
	os.MkdirAll(config.ProcessDir, 0755)
	os.MkdirAll(config.FailedDir, 0755)

	go watchFiles(config)

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	router.GET("/api/v1/sales", getSales)
	router.GET("/api/v1/sales/:id", getSaleByID)
	router.POST("/api/v1/sales", createSale)

	router.GET("/api/v1/products", getProducts)
	router.GET("/api/v1/products/:id", getProductByID)
	router.POST("/api/v1/products", createProduct)
	router.PUT("/api/v1/products/:id", updateProduct)
	router.DELETE("/api/v1/products/:id", deleteProduct)

	router.POST("/api/v1/admin/ingest", triggerIngest)

	fmt.Printf("Server running on http://localhost:%s\n", config.Port)
	fmt.Printf("Watching for files in: %s\n", config.WatchDir)
	router.Run(":" + config.Port)
}

func getSales(c *gin.Context) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	city := c.Query("city")
	region := c.Query("region")

	result := []SaleEvent{}
	for _, sale := range db.sales {
		if city != "" && sale.City != city {
			continue
		}
		if region != "" && sale.Region != region {
			continue
		}
		result = append(result, sale)
		if len(result) >= limit {
			break
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    result,
		"meta":    gin.H{"total": len(db.sales), "count": len(result)},
	})
}

func getSaleByID(c *gin.Context) {
	id := c.Param("id")

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, sale := range db.sales {
		if sale.SaleID == id {
			c.JSON(http.StatusOK, gin.H{"success": true, "data": sale})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "Sale not found"})
}

func createSale(c *gin.Context) {
	var sales []SaleEvent

	if err := c.ShouldBindJSON(&sales); err != nil {
		var single SaleEvent
		if err := c.ShouldBindJSON(&single); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "Invalid JSON"})
			return
		}
		sales = []SaleEvent{single}
	}

	db.mu.Lock()
	for i := range sales {
		sales[i].Source = "api"
	}
	db.sales = append(db.sales, sales...)
	db.mu.Unlock()

	c.JSON(http.StatusCreated, gin.H{"success": true, "data": gin.H{"ingested": len(sales)}})
}

func getProducts(c *gin.Context) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	products := []Product{}
	for _, p := range db.products {
		products = append(products, p)
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": products})
}

func getProductByID(c *gin.Context) {
	id := c.Param("id")

	db.mu.RLock()
	product, exists := db.products[id]
	db.mu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "Product not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": product})
}

func createProduct(c *gin.Context) {
	var product Product
	if err := c.ShouldBindJSON(&product); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	db.mu.Lock()
	if _, exists := db.products[product.ID]; exists {
		db.mu.Unlock()
		c.JSON(http.StatusConflict, gin.H{"success": false, "error": "Product already exists"})
		return
	}
	product.CreatedAt = time.Now()
	db.products[product.ID] = product
	db.mu.Unlock()

	c.JSON(http.StatusCreated, gin.H{"success": true, "data": product})
}

func updateProduct(c *gin.Context) {
	id := c.Param("id")

	var updates Product
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
		return
	}

	db.mu.Lock()
	product, exists := db.products[id]
	if !exists {
		db.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "Product not found"})
		return
	}

	if updates.Name != "" {
		product.Name = updates.Name
	}
	if updates.Description != "" {
		product.Description = updates.Description
	}
	if updates.Price > 0 {
		product.Price = updates.Price
	}
	db.products[id] = product
	db.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"success": true, "data": product})
}

func deleteProduct(c *gin.Context) {
	id := c.Param("id")

	db.mu.Lock()
	if _, exists := db.products[id]; !exists {
		db.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "Product not found"})
		return
	}
	delete(db.products, id)
	db.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"deleted": true}})
}

func watchFiles(config Config) {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		processNewFiles(config)
	}
}

func processNewFiles(config Config) {
	files, err := os.ReadDir(config.WatchDir)
	if err != nil {
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		ext := strings.ToLower(filepath.Ext(filename))

		if ext != ".csv" && ext != ".json" {
			continue
		}

		fullPath := filepath.Join(config.WatchDir, filename)
		fmt.Printf("Processing file: %s\n", filename)

		sales, err := parseFile(fullPath, ext)
		if err != nil {
			fmt.Printf("Error parsing %s: %v\n", filename, err)
			os.Rename(fullPath, filepath.Join(config.FailedDir, filename))
			continue
		}

		db.mu.Lock()
		db.sales = append(db.sales, sales...)
		db.mu.Unlock()

		fmt.Printf("Ingested %d records from %s\n", len(sales), filename)
		os.Rename(fullPath, filepath.Join(config.ProcessDir, filename))
	}
}

func triggerIngest(c *gin.Context) {
	config := loadConfig()
	processNewFiles(config)
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Ingestion triggered"})
}

func parseFile(path string, ext string) ([]SaleEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if ext == ".csv" {
		return parseCSV(file)
	}
	return parseJSON(file)
}

func parseCSV(file *os.File) ([]SaleEvent, error) {
	reader := csv.NewReader(file)

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[strings.TrimSpace(col)] = i
	}

	var sales []SaleEvent

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		amount, _ := strconv.ParseFloat(row[colIndex["amount"]], 64)
		eventTime, _ := strconv.ParseInt(row[colIndex["event_time"]], 10, 64)

		sale := SaleEvent{
			SaleID:       row[colIndex["sale_id"]],
			SalesmanID:   row[colIndex["salesman_id"]],
			SalesmanName: row[colIndex["salesman_name"]],
			City:         row[colIndex["city"]],
			Region:       row[colIndex["region"]],
			ProductID:    row[colIndex["product_id"]],
			Amount:       amount,
			EventTime:    eventTime,
			Source:       "file",
		}
		sales = append(sales, sale)
	}

	return sales, nil
}

func parseJSON(file *os.File) ([]SaleEvent, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var rawSales []struct {
		SaleID       string  `json:"sale_id"`
		SalesmanID   string  `json:"salesman_id"`
		SalesmanName string  `json:"salesman_name"`
		City         string  `json:"city"`
		Region       string  `json:"region"`
		ProductID    string  `json:"product_id"`
		Amount       float64 `json:"amount"`
		EventTime    int64   `json:"event_time"`
	}

	if err := json.Unmarshal(data, &rawSales); err != nil {
		return nil, err
	}

	sales := make([]SaleEvent, len(rawSales))
	for i, raw := range rawSales {
		sales[i] = SaleEvent{
			SaleID:       raw.SaleID,
			SalesmanID:   raw.SalesmanID,
			SalesmanName: raw.SalesmanName,
			City:         raw.City,
			Region:       raw.Region,
			ProductID:    raw.ProductID,
			Amount:       raw.Amount,
			EventTime:    raw.EventTime,
			Source:       "file",
		}
	}

	return sales, nil
}
