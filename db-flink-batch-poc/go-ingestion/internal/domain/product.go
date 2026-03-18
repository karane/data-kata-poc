package domain

import "time"

// Product represents a product entity for CRUD operations.
type Product struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Category    string    `json:"category,omitempty"`
	Price       float64   `json:"price"`
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// CreateProductRequest for POST /api/v1/products.
type CreateProductRequest struct {
	ID          string  `json:"id" binding:"required"`
	Name        string  `json:"name" binding:"required"`
	Description string  `json:"description"`
	Category    string  `json:"category"`
	Price       float64 `json:"price" binding:"gte=0"`
}

// UpdateProductRequest for PUT /api/v1/products/:id.
type UpdateProductRequest struct {
	Name        *string  `json:"name"`
	Description *string  `json:"description"`
	Category    *string  `json:"category"`
	Price       *float64 `json:"price"`
	Active      *bool    `json:"active"`
}
