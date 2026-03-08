package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	RustFSEndpoint string
	AccessKey      string
	SecretKey      string
	Bucket         string
	FromDate       string // yyyy-MM-dd
	ToDate         string // yyyy-MM-dd
	RecordsPerFile int
	SalesmenCount  int
}

type Salesman struct {
	ID   string
	Name string
}

type CityRegion struct {
	City   string
	Region string
}

var cityRegions = []CityRegion{
	{"New York", "Northeast"}, {"Boston", "Northeast"}, {"Philadelphia", "Northeast"},
	{"Washington DC", "Northeast"}, {"Los Angeles", "West"}, {"San Francisco", "West"},
	{"Seattle", "West"}, {"San Diego", "West"}, {"Chicago", "Midwest"},
	{"Detroit", "Midwest"}, {"Minneapolis", "Midwest"}, {"Houston", "South"},
	{"Atlanta", "South"}, {"Dallas", "South"}, {"Miami", "South"},
	{"San Antonio", "South"}, {"Phoenix", "Southwest"}, {"Denver", "Southwest"},
	{"Las Vegas", "Southwest"},
}

var firstNames = []string{"Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry", "Iris", "Jack"}
var lastNames = []string{"Johnson", "Williams", "Davis", "Brown", "Martinez", "Wilson", "Taylor", "Anderson", "Thomas", "Harris"}

func loadConfig() Config {
	return Config{
		RustFSEndpoint: envOrDefault("RUSTFS_ENDPOINT", "http://rustfs:9000"),
		AccessKey:      envOrDefault("RUSTFS_ACCESS_KEY", "rustfsaccess"),
		SecretKey:      envOrDefault("RUSTFS_SECRET_KEY", "rustfssecret"),
		Bucket:         envOrDefault("RUSTFS_BUCKET", "sales-csv"),
		FromDate:       envOrDefault("GEN_FROM_DATE", "2024-02-01"),
		ToDate:         envOrDefault("GEN_TO_DATE", "2024-02-07"),
		RecordsPerFile: envOrDefaultInt("RECORDS_PER_FILE", 30),
		SalesmenCount:  envOrDefaultInt("SALESMEN_COUNT", 8),
	}
}

func buildSalesmenPool(n int) []Salesman {
	pool := make([]Salesman, n)
	for i := range pool {
		pool[i] = Salesman{
			ID:   fmt.Sprintf("SM%03d", i+1),
			Name: firstNames[i%len(firstNames)] + " " + lastNames[(i+3)%len(lastNames)],
		}
	}
	return pool
}

func generateCSV(date time.Time, salesmen []Salesman, recordsPerFile int) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	_ = w.Write([]string{"saleId", "salesmanId", "salesmanName", "city", "region", "productId", "amount", "eventTime"})

	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
	dayEnd := time.Date(date.Year(), date.Month(), date.Day(), 23, 59, 59, 0, time.UTC).UnixMilli()
	dayRange := dayEnd - dayStart

	products := make([]string, 8)
	for i := range products {
		products[i] = fmt.Sprintf("P%03d", i+1)
	}

	for i := 0; i < recordsPerFile; i++ {
		sm := salesmen[rand.Intn(len(salesmen))]
		cr := cityRegions[rand.Intn(len(cityRegions))]
		product := products[rand.Intn(len(products))]
		amount := 100.0 + rand.Float64()*4900.0
		eventTime := dayStart + rand.Int63n(dayRange)
		saleID := fmt.Sprintf("RUSTFS%s%05d", date.Format("20060102"), i+1)

		_ = w.Write([]string{
			saleID,
			sm.ID,
			sm.Name,
			cr.City,
			cr.Region,
			product,
			fmt.Sprintf("%.2f", amount),
			strconv.FormatInt(eventTime, 10),
		})
	}

	w.Flush()
	return buf.Bytes()
}

func run(cfg Config) error {
	ctx := context.Background()

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(cfg.RustFSEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		UsePathStyle: true,
	})

	// Ensure bucket exists (RustFS doesn't auto-create)
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
	if err != nil {
		// Ignore "already exists" errors
		errStr := err.Error()
		if !strings.Contains(errStr, "BucketAlreadyOwnedByYou") && !strings.Contains(errStr, "BucketAlreadyExists") {
			return fmt.Errorf("create bucket %s: %w", cfg.Bucket, err)
		}
	}
	log.Printf("[sales-csv-generator] bucket %q ready", cfg.Bucket)

	salesmen := buildSalesmenPool(cfg.SalesmenCount)
	log.Printf("[sales-csv-generator] salesmen pool (%d):", len(salesmen))
	for _, s := range salesmen {
		log.Printf("  %s  %s", s.ID, s.Name)
	}

	from, err := time.Parse("2006-01-02", cfg.FromDate)
	if err != nil {
		return fmt.Errorf("invalid GEN_FROM_DATE %q: %w", cfg.FromDate, err)
	}
	to, err := time.Parse("2006-01-02", cfg.ToDate)
	if err != nil {
		return fmt.Errorf("invalid GEN_TO_DATE %q: %w", cfg.ToDate, err)
	}

	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		filename := fmt.Sprintf("sales_%s.csv", d.Format("20060102"))
		content := generateCSV(d, salesmen, cfg.RecordsPerFile)

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(cfg.Bucket),
			Key:           aws.String(filename),
			Body:          bytes.NewReader(content),
			ContentLength: aws.Int64(int64(len(content))),
			ContentType:   aws.String("text/csv"),
		})
		if err != nil {
			return fmt.Errorf("upload %s: %w", filename, err)
		}
		log.Printf("[sales-csv-generator] uploaded s3://%s/%s (%d records)", cfg.Bucket, filename, cfg.RecordsPerFile)
	}

	log.Println("[sales-csv-generator] done.")
	return nil
}

func main() {
	cfg := loadConfig()
	log.Printf("[sales-csv-generator] endpoint=%s  bucket=%s  from=%s  to=%s  records=%d",
		cfg.RustFSEndpoint, cfg.Bucket, cfg.FromDate, cfg.ToDate, cfg.RecordsPerFile)

	if err := run(cfg); err != nil {
		log.Fatalf("[sales-csv-generator] ERROR: %v", err)
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
