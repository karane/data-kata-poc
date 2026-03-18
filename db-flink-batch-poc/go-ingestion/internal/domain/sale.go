package domain

// SaleEvent aligns with the Java SaleEvent schema used by Flink.
// This is the primary data transfer object for sales data.
type SaleEvent struct {
	SaleID       string  `json:"saleId"`
	SalesmanID   string  `json:"salesmanId"`
	SalesmanName string  `json:"salesmanName"`
	City         string  `json:"city"`
	Region       string  `json:"region"`
	ProductID    string  `json:"productId"`
	Amount       float64 `json:"amount"`
	EventTime    int64   `json:"eventTime"` // epoch milliseconds (UTC)
	Source       string  `json:"source"`    // "file" for this service
}

// SaleEventFromFile represents a raw sale record from file parsing
// before enrichment with source metadata.
type SaleEventFromFile struct {
	SaleID       string  `json:"sale_id" csv:"sale_id"`
	SalesmanID   string  `json:"salesman_id" csv:"salesman_id"`
	SalesmanName string  `json:"salesman_name" csv:"salesman_name"`
	City         string  `json:"city" csv:"city"`
	Region       string  `json:"region" csv:"region"`
	ProductID    string  `json:"product_id" csv:"product_id"`
	Amount       float64 `json:"amount" csv:"amount"`
	EventTime    int64   `json:"event_time" csv:"event_time"`
}

// ToSaleEvent converts file record to SaleEvent with source annotation.
func (s *SaleEventFromFile) ToSaleEvent() SaleEvent {
	return SaleEvent{
		SaleID:       s.SaleID,
		SalesmanID:   s.SalesmanID,
		SalesmanName: s.SalesmanName,
		City:         s.City,
		Region:       s.Region,
		ProductID:    s.ProductID,
		Amount:       s.Amount,
		EventTime:    s.EventTime,
		Source:       "file",
	}
}

// SalesFilter contains filtering options for sales queries.
type SalesFilter struct {
	From   *int64  // inclusive lower bound (epoch ms)
	To     *int64  // inclusive upper bound (epoch ms)
	City   *string // filter by city
	Region *string // filter by region
	Limit  int     // page size
	Cursor string  // cursor for pagination
}
