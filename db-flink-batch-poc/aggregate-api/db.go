package main

import (
	"database/sql"
)

type Store interface {
	TopSalesByCity() ([]CitySales, error)
	TopSalesman() (Salesman, error)
}

type PostgresStore struct {
	DB *sql.DB
}

type CitySales struct {
	City       string  `json:"city"`
	Total      float64 `json:"total_sales"`
	Rank       int     `json:"rank"`
	WindowEnd  string  `json:"window_end"`
}

type Salesman struct {
	Name       string  `json:"name"`
	ID         string  `json:"id"`
	Total      float64 `json:"total_sales"`
	Rank       int     `json:"rank"`
	WindowEnd  string  `json:"window_end"`
}

// Top cities from latest window
func (p *PostgresStore) TopSalesByCity() ([]CitySales, error) {
	rows, err := p.DB.Query(`
		SELECT city, total_sales, rank, window_end
		FROM top_cities_latest
		WHERE window_end = (SELECT MAX(window_end) FROM top_cities_latest)
		ORDER BY rank ASC
		LIMIT 10;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CitySales

	for rows.Next() {
		var c CitySales
		err := rows.Scan(&c.City, &c.Total, &c.Rank, &c.WindowEnd)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	return result, nil
}

// Top salesman (rank = 1 from latest window)
func (p *PostgresStore) TopSalesman() (Salesman, error) {
	row := p.DB.QueryRow(`
		SELECT salesman_name, salesman_id, total_sales, rank, window_end
		FROM top_salesmen_latest
		WHERE window_end = (SELECT MAX(window_end) FROM top_salesmen_latest)
		ORDER BY rank ASC
		LIMIT 1;
	`)

	var s Salesman
	err := row.Scan(&s.Name, &s.ID, &s.Total, &s.Rank, &s.WindowEnd)
	return s, err
}