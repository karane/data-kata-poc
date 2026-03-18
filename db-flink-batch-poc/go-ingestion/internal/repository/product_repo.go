package repository

import (
	"sync"
	"time"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
)

// ProductRepository defines the interface for product storage.
type ProductRepository interface {
	Create(product domain.Product) error
	Update(id string, req domain.UpdateProductRequest) (*domain.Product, error)
	Delete(id string) error
	FindByID(id string) (*domain.Product, error)
	FindAll() ([]domain.Product, error)
	Count() int
}

// InMemoryProductRepo is an in-memory implementation of ProductRepository.
type InMemoryProductRepo struct {
	mu       sync.RWMutex
	products map[string]domain.Product
}

// NewInMemoryProductRepo creates a new in-memory product repository.
func NewInMemoryProductRepo() *InMemoryProductRepo {
	return &InMemoryProductRepo{
		products: make(map[string]domain.Product),
	}
}

// Create stores a new product.
func (r *InMemoryProductRepo) Create(product domain.Product) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.products[product.ID]; exists {
		return domain.ErrAlreadyExists
	}

	now := time.Now()
	product.CreatedAt = now
	product.UpdatedAt = now
	product.Active = true

	r.products[product.ID] = product
	return nil
}

// Update modifies an existing product.
func (r *InMemoryProductRepo) Update(id string, req domain.UpdateProductRequest) (*domain.Product, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	product, exists := r.products[id]
	if !exists {
		return nil, domain.ErrNotFound
	}

	if req.Name != nil {
		product.Name = *req.Name
	}
	if req.Description != nil {
		product.Description = *req.Description
	}
	if req.Category != nil {
		product.Category = *req.Category
	}
	if req.Price != nil {
		product.Price = *req.Price
	}
	if req.Active != nil {
		product.Active = *req.Active
	}

	product.UpdatedAt = time.Now()
	r.products[id] = product

	return &product, nil
}

// Delete removes a product by ID.
func (r *InMemoryProductRepo) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.products[id]; !exists {
		return domain.ErrNotFound
	}

	delete(r.products, id)
	return nil
}

// FindByID retrieves a product by ID.
func (r *InMemoryProductRepo) FindByID(id string) (*domain.Product, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	product, exists := r.products[id]
	if !exists {
		return nil, domain.ErrNotFound
	}
	return &product, nil
}

// FindAll retrieves all products.
func (r *InMemoryProductRepo) FindAll() ([]domain.Product, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	products := make([]domain.Product, 0, len(r.products))
	for _, p := range r.products {
		products = append(products, p)
	}
	return products, nil
}

// Count returns the total number of products.
func (r *InMemoryProductRepo) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.products)
}
