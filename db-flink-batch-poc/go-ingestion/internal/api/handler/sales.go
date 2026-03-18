package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
	"github.com/data-kata-poc/go-ingestion/internal/service"
)

// SalesHandler handles sales-related HTTP requests.
type SalesHandler struct {
	service *service.SalesService
}

// NewSalesHandler creates a new SalesHandler.
func NewSalesHandler(svc *service.SalesService) *SalesHandler {
	return &SalesHandler{service: svc}
}

// List returns paginated sales with optional filtering.
// GET /api/v1/sales?from=&to=&limit=&cursor=&city=&region=
func (h *SalesHandler) List(c *gin.Context) {
	filter := domain.SalesFilter{
		Limit: 100, // default
	}

	// Parse from
	if fromStr := c.Query("from"); fromStr != "" {
		from, err := strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "INVALID_FROM",
					"message": "from must be a valid epoch milliseconds",
				},
			})
			return
		}
		filter.From = &from
	}

	// Parse to
	if toStr := c.Query("to"); toStr != "" {
		to, err := strconv.ParseInt(toStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "INVALID_TO",
					"message": "to must be a valid epoch milliseconds",
				},
			})
			return
		}
		filter.To = &to
	}

	// Parse limit
	if limitStr := c.Query("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit < 1 || limit > 1000 {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "INVALID_LIMIT",
					"message": "limit must be between 1 and 1000",
				},
			})
			return
		}
		filter.Limit = limit
	}

	// Parse cursor
	filter.Cursor = c.Query("cursor")

	// Parse city
	if city := c.Query("city"); city != "" {
		filter.City = &city
	}

	// Parse region
	if region := c.Query("region"); region != "" {
		filter.Region = &region
	}

	sales, nextCursor, total, err := h.service.List(filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to retrieve sales",
			},
		})
		return
	}

	response := gin.H{
		"success": true,
		"data": gin.H{
			"sales": sales,
		},
		"meta": gin.H{
			"total":   total,
			"count":   len(sales),
			"hasMore": nextCursor != "",
		},
	}

	if nextCursor != "" {
		response["meta"].(gin.H)["nextCursor"] = nextCursor
	}

	c.JSON(http.StatusOK, response)
}

// GetByID returns a single sale by ID.
// GET /api/v1/sales/:id
func (h *SalesHandler) GetByID(c *gin.Context) {
	id := c.Param("id")

	sale, err := h.service.GetByID(id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"success": false,
				"error": gin.H{
					"code":    "NOT_FOUND",
					"message": "sale not found",
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to retrieve sale",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    sale,
	})
}

// BatchIngest ingests multiple sales at once.
// POST /api/v1/sales/batch
func (h *SalesHandler) BatchIngest(c *gin.Context) {
	var sales []domain.SaleEvent
	if err := c.ShouldBindJSON(&sales); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INVALID_INPUT",
				"message": "invalid JSON body",
			},
		})
		return
	}

	// Set source for all
	for i := range sales {
		sales[i].Source = "file"
	}

	if err := h.service.IngestBatch(sales); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"code":    "INTERNAL_ERROR",
				"message": "failed to ingest sales",
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"success": true,
		"data": gin.H{
			"ingested": len(sales),
		},
	})
}
