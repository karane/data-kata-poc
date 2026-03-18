package parser

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
)

// Parser interface for file parsing.
type Parser interface {
	Parse(r io.Reader) ([]domain.SaleEventFromFile, error)
	SupportedExtensions() []string
}

// Registry manages parsers by file extension.
type Registry struct {
	parsers map[string]Parser
}

// NewRegistry creates a new parser registry with default parsers.
func NewRegistry() *Registry {
	r := &Registry{parsers: make(map[string]Parser)}
	r.Register(NewCSVParser())
	r.Register(NewJSONParser())
	return r
}

// Register adds a parser to the registry.
func (r *Registry) Register(p Parser) {
	for _, ext := range p.SupportedExtensions() {
		r.parsers[strings.ToLower(ext)] = p
	}
}

// ParserFor returns the appropriate parser for the given filename.
func (r *Registry) ParserFor(filename string) (Parser, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	if p, ok := r.parsers[ext]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("no parser for extension: %s", ext)
}

// SupportedExtensions returns all registered extensions.
func (r *Registry) SupportedExtensions() []string {
	exts := make([]string, 0, len(r.parsers))
	for ext := range r.parsers {
		exts = append(exts, ext)
	}
	return exts
}
