package api

import (
	"github.com/gin-gonic/gin"

	"github.com/data-kata-poc/go-ingestion/internal/api/handler"
	"github.com/data-kata-poc/go-ingestion/internal/api/middleware"
	"github.com/data-kata-poc/go-ingestion/internal/service"
)

// Router holds all HTTP handlers and sets up routes.
type Router struct {
	engine         *gin.Engine
	healthHandler  *handler.HealthHandler
	salesHandler   *handler.SalesHandler
	productHandler *handler.ProductHandler
}

// NewRouter creates a new Router with all handlers.
func NewRouter(
	salesService *service.SalesService,
	productService *service.ProductService,
) *Router {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.Logger())

	return &Router{
		engine:         engine,
		healthHandler:  handler.NewHealthHandler(),
		salesHandler:   handler.NewSalesHandler(salesService),
		productHandler: handler.NewProductHandler(productService),
	}
}

// Setup configures all routes.
func (r *Router) Setup() {
	// Health endpoints
	r.engine.GET("/health", r.healthHandler.Health)
	r.engine.GET("/ready", r.healthHandler.Ready)

	// API v1
	v1 := r.engine.Group("/api/v1")
	{
		// Sales endpoints
		sales := v1.Group("/sales")
		{
			sales.GET("", r.salesHandler.List)
			sales.GET("/:id", r.salesHandler.GetByID)
			sales.POST("/batch", r.salesHandler.BatchIngest)
		}

		// Product endpoints
		products := v1.Group("/products")
		{
			products.GET("", r.productHandler.List)
			products.GET("/:id", r.productHandler.GetByID)
			products.POST("", r.productHandler.Create)
			products.PUT("/:id", r.productHandler.Update)
			products.DELETE("/:id", r.productHandler.Delete)
		}
	}
}

// Engine returns the underlying gin engine.
func (r *Router) Engine() *gin.Engine {
	return r.engine
}
