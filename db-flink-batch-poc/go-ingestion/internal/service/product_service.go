package service

import (
	"github.com/data-kata-poc/go-ingestion/internal/domain"
	"github.com/data-kata-poc/go-ingestion/internal/repository"
)

// ProductService handles business logic for products.
type ProductService struct {
	repo repository.ProductRepository
}

// NewProductService creates a new ProductService.
func NewProductService(repo repository.ProductRepository) *ProductService {
	return &ProductService{repo: repo}
}

// Create adds a new product.
func (s *ProductService) Create(req domain.CreateProductRequest) (*domain.Product, error) {
	product := domain.Product{
		ID:          req.ID,
		Name:        req.Name,
		Description: req.Description,
		Category:    req.Category,
		Price:       req.Price,
	}

	if err := s.repo.Create(product); err != nil {
		return nil, err
	}

	return s.repo.FindByID(req.ID)
}

// Update modifies an existing product.
func (s *ProductService) Update(id string, req domain.UpdateProductRequest) (*domain.Product, error) {
	return s.repo.Update(id, req)
}

// Delete removes a product.
func (s *ProductService) Delete(id string) error {
	return s.repo.Delete(id)
}

// GetByID retrieves a product by ID.
func (s *ProductService) GetByID(id string) (*domain.Product, error) {
	return s.repo.FindByID(id)
}

// List retrieves all products.
func (s *ProductService) List() ([]domain.Product, error) {
	return s.repo.FindAll()
}
