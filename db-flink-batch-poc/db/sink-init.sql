-- Sink table
CREATE TABLE IF NOT EXISTS sales_ranks (
    id            SERIAL         PRIMARY KEY,
    rank_type     VARCHAR(10)    NOT NULL,  -- 'CITY' | 'COUNTRY'
    group_key     VARCHAR(200)   NOT NULL,  -- city name or salesman name
    entity_id     VARCHAR(50),              -- salesmanId (COUNTRY stream only)
    total_sales   DECIMAL(12,2)  NOT NULL,
    window_start  TIMESTAMP      NOT NULL,  -- earliest event_time in this group
    window_end    TIMESTAMP      NOT NULL,  -- latest  event_time in this group
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    UNIQUE (rank_type, group_key, window_end)
);

-- Convenience views for querying results
CREATE OR REPLACE VIEW top_cities_latest AS
    SELECT
        group_key AS city,
        total_sales,
        RANK() OVER (PARTITION BY window_end ORDER BY total_sales DESC)   AS rank,
        window_end
    FROM sales_ranks
    WHERE rank_type = 'CITY'
    ORDER BY window_end DESC, total_sales DESC;

CREATE OR REPLACE VIEW top_salesmen_latest AS
    SELECT
        group_key AS salesman_name,
        entity_id AS salesman_id,
        total_sales,
        RANK() OVER (PARTITION BY window_end ORDER BY total_sales DESC)   AS rank,
        window_end
    FROM sales_ranks
    WHERE rank_type = 'COUNTRY'
    ORDER BY window_end DESC, total_sales DESC;
