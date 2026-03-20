# Go Ingestion Service

File-based data ingestion service with REST API for sales and products.

## Features

- REST API for Sales and Products CRUD
- File watcher (CSV/JSON) - auto-ingests from `data/inbox/`
- Data generator with configurable output

## Run

```bash
go run .
```

Server starts at `http://localhost:8080`

## Generate Test Data

```bash
go run ./cmd/generator -count 100 -output data/inbox/sales.json -pretty
```

Options:
- `-count` - number of records (default: 100)
- `-output` - file path (default: stdout)
- `-pretty` - format JSON

## API Endpoints

### Sales

```bash
GET    /api/v1/sales              # List (filters: ?city=&region=&limit=)
GET    /api/v1/sales/:id          # Get by ID
POST   /api/v1/sales              # Create
```

### Products

```bash
GET    /api/v1/products           # List
GET    /api/v1/products/:id       # Get by ID
POST   /api/v1/products           # Create
PUT    /api/v1/products/:id       # Update
DELETE /api/v1/products/:id       # Delete
```

### Admin

```bash
GET    /health                    # Health check
POST   /api/v1/admin/ingest       # Trigger file ingestion
```

## Examples

```bash
# Create product
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{"id":"P001","name":"Laptop","price":999.99}'

# List sales
curl http://localhost:8080/api/v1/sales

# Filter by city
curl "http://localhost:8080/api/v1/sales?city=New%20York"
```

## Docker

```bash
docker-compose up --build
```
