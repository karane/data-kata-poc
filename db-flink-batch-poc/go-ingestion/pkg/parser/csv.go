package parser

import (
	"io"

	"github.com/gocarina/gocsv"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
)

// CSVParser parses CSV files into SaleEventFromFile records.
type CSVParser struct{}

// NewCSVParser creates a new CSV parser.
func NewCSVParser() *CSVParser {
	return &CSVParser{}
}

// Parse reads CSV data and returns sale events.
func (p *CSVParser) Parse(r io.Reader) ([]domain.SaleEventFromFile, error) {
	var sales []domain.SaleEventFromFile
	if err := gocsv.Unmarshal(r, &sales); err != nil {
		return nil, err
	}
	return sales, nil
}

// SupportedExtensions returns the file extensions this parser handles.
func (p *CSVParser) SupportedExtensions() []string {
	return []string{".csv"}
}
