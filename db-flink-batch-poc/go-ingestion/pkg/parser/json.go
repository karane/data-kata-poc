package parser

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
)

// JSONParser parses JSON array files into SaleEventFromFile records.
type JSONParser struct{}

// NewJSONParser creates a new JSON parser.
func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

// Parse reads JSON data and returns sale events.
// Supports both JSON arrays and JSON Lines format.
func (p *JSONParser) Parse(r io.Reader) ([]domain.SaleEventFromFile, error) {
	// Read all content to determine format
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	content := strings.TrimSpace(string(data))

	// Check if it's a JSON array
	if strings.HasPrefix(content, "[") {
		var sales []domain.SaleEventFromFile
		if err := json.Unmarshal(data, &sales); err != nil {
			return nil, err
		}
		return sales, nil
	}

	// Otherwise treat as JSON Lines
	return p.parseJSONLines(strings.NewReader(content))
}

// parseJSONLines parses newline-delimited JSON.
func (p *JSONParser) parseJSONLines(r io.Reader) ([]domain.SaleEventFromFile, error) {
	var sales []domain.SaleEventFromFile
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var sale domain.SaleEventFromFile
		if err := json.Unmarshal([]byte(line), &sale); err != nil {
			return nil, err
		}
		sales = append(sales, sale)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return sales, nil
}

// SupportedExtensions returns the file extensions this parser handles.
func (p *JSONParser) SupportedExtensions() []string {
	return []string{".json", ".jsonl"}
}
