package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// HealthHandler handles health check endpoints.
type HealthHandler struct{}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// Health returns service health status.
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// Ready returns service readiness status.
func (h *HealthHandler) Ready(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}
