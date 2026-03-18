package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
	"github.com/data-kata-poc/go-ingestion/internal/service"
)

// ProductHandler handles product-related HTTP requests.
type ProductHandler struct {
	service *service.ProductService
}

// NewProductHandler creates a new ProductHandler.
func NewProductHandler(svc *service.ProductService) *ProductHandler {
	return &ProductHandler{service: svc}
}

// List returns all products.
// GET /api/v1/products
func (h *ProductHandler) List(c *gin.Context) {
	products, err := h.service.List()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to retrieve products",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"products": products,
		},
		"meta": gin.H{
			"total": len(products),
		},
	})
}

// GetByID returns a single product by ID.
// GET /api/v1/products/:id
func (h *ProductHandler) GetByID(c *gin.Context) {
	id := c.Param("id")

	product, err := h.service.GetByID(id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "NOT_FOUND",
					"message": "product not found",
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to retrieve product",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    product,
	})
}

// Create adds a new product.
// POST /api/v1/products
func (h *ProductHandler) Create(c *gin.Context) {
	var req domain.CreateProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INVALID_INPUT",
				"message": err.Error(),
			},
		})
		return
	}

	product, err := h.service.Create(req)
	if err != nil {
		if errors.Is(err, domain.ErrAlreadyExists) {
			c.JSON(http.StatusConflict, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "ALREADY_EXISTS",
					"message": "product with this ID already exists",
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to create product",
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data":    product,
	})
}

// Update modifies an existing product.
// PUT /api/v1/products/:id
func (h *ProductHandler) Update(c *gin.Context) {
	id := c.Param("id")

	var req domain.UpdateProductRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INVALID_INPUT",
				"message": err.Error(),
			},
		})
		return
	}

	product, err := h.service.Update(id, req)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "NOT_FOUND",
					"message": "product not found",
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to update product",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    product,
	})
}

// Delete removes a product.
// DELETE /api/v1/products/:id
func (h *ProductHandler) Delete(c *gin.Context) {
	id := c.Param("id")

	err := h.service.Delete(id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "NOT_FOUND",
					"message": "product not found",
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to delete product",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"deleted": true,
		},
	})
}
