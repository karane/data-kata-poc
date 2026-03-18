package repository

import (
	"sort"
	"sync"

	"github.com/data-kata-poc/go-ingestion/internal/domain"
)

// SalesRepository defines the interface for sales storage.
type SalesRepository interface {
	Save(sale domain.SaleEvent) error
	SaveBatch(sales []domain.SaleEvent) error
	FindByID(id string) (*domain.SaleEvent, error)
	FindAll(filter domain.SalesFilter) ([]domain.SaleEvent, string, error)
	Count() int
}

// InMemorySalesRepo is an in-memory implementation of SalesRepository.
type InMemorySalesRepo struct {
	mu    sync.RWMutex
	sales map[string]domain.SaleEvent
	order []string // maintains insertion order for cursor pagination
}

// NewInMemorySalesRepo creates a new in-memory sales repository.
func NewInMemorySalesRepo() *InMemorySalesRepo {
	return &InMemorySalesRepo{
		sales: make(map[string]domain.SaleEvent),
		order: make([]string, 0),
	}
}

// Save stores a single sale event.
func (r *InMemorySalesRepo) Save(sale domain.SaleEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.sales[sale.SaleID]; !exists {
		r.order = append(r.order, sale.SaleID)
	}
	r.sales[sale.SaleID] = sale
	return nil
}

// SaveBatch stores multiple sale events.
func (r *InMemorySalesRepo) SaveBatch(sales []domain.SaleEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sale := range sales {
		if _, exists := r.sales[sale.SaleID]; !exists {
			r.order = append(r.order, sale.SaleID)
		}
		r.sales[sale.SaleID] = sale
	}
	return nil
}

// FindByID retrieves a sale by its ID.
func (r *InMemorySalesRepo) FindByID(id string) (*domain.SaleEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sale, exists := r.sales[id]
	if !exists {
		return nil, domain.ErrNotFound
	}
	return &sale, nil
}

// FindAll retrieves sales matching the filter criteria.
func (r *InMemorySalesRepo) FindAll(filter domain.SalesFilter) ([]domain.SaleEvent, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Collect all sales sorted by event time
	all := make([]domain.SaleEvent, 0, len(r.sales))
	for _, sale := range r.sales {
		all = append(all, sale)
	}

	// Sort by event time ascending
	sort.Slice(all, func(i, j int) bool {
		return all[i].EventTime < all[j].EventTime
	})

	// Apply filters
	filtered := make([]domain.SaleEvent, 0)
	startIndex := 0

	// Find cursor position
	if filter.Cursor != "" {
		for i, sale := range all {
			if sale.SaleID == filter.Cursor {
				startIndex = i + 1
				break
			}
		}
	}

	for i := startIndex; i < len(all); i++ {
		sale := all[i]

		// Apply time range filter
		if filter.From != nil && sale.EventTime < *filter.From {
			continue
		}
		if filter.To != nil && sale.EventTime > *filter.To {
			continue
		}

		// Apply city filter
		if filter.City != nil && sale.City != *filter.City {
			continue
		}

		// Apply region filter
		if filter.Region != nil && sale.Region != *filter.Region {
			continue
		}

		filtered = append(filtered, sale)

		// Check limit
		if filter.Limit > 0 && len(filtered) >= filter.Limit {
			break
		}
	}

	// Determine next cursor
	var nextCursor string
	if len(filtered) > 0 && filter.Limit > 0 && len(filtered) == filter.Limit {
		nextCursor = filtered[len(filtered)-1].SaleID
	}

	return filtered, nextCursor, nil
}

// Count returns the total number of sales.
func (r *InMemorySalesRepo) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sales)
}
