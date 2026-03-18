package service

import (
	"github.com/data-kata-poc/go-ingestion/internal/domain"
	"github.com/data-kata-poc/go-ingestion/internal/repository"
)

// SalesService handles business logic for sales.
type SalesService struct {
	repo repository.SalesRepository
}

// NewSalesService creates a new SalesService.
func NewSalesService(repo repository.SalesRepository) *SalesService {
	return &SalesService{repo: repo}
}

// GetByID retrieves a sale by ID.
func (s *SalesService) GetByID(id string) (*domain.SaleEvent, error) {
	return s.repo.FindByID(id)
}

// List retrieves sales matching the filter.
func (s *SalesService) List(filter domain.SalesFilter) ([]domain.SaleEvent, string, int, error) {
	sales, nextCursor, err := s.repo.FindAll(filter)
	if err != nil {
		return nil, "", 0, err
	}
	total := s.repo.Count()
	return sales, nextCursor, total, nil
}

// IngestBatch stores multiple sales from file ingestion.
func (s *SalesService) IngestBatch(sales []domain.SaleEvent) error {
	return s.repo.SaveBatch(sales)
}

// Ingest stores a single sale.
func (s *SalesService) Ingest(sale domain.SaleEvent) error {
	return s.repo.Save(sale)
}
