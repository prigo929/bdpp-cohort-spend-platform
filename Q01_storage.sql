-- ============================================================================
-- SDBIS Master's Programme · Big Data: Persistence Systems & Languages
-- Checkpoint 1: Storage Layer Design Calculations for Question 1
-- Team: Q01 (Customer Cohort Spend Evolution)
-- Authors: Prigoreanu Alin-Gabriel, Bîrgovan A. Mihai-Daniel, Schipor Ana-Maria
-- ============================================================================
-- Run this script in DuckDB against the baseline dataset:
--     docker compose exec lab duckdb
--     .read minio.sql
--     .read Q01_numbers.sql
-- ============================================================================

.mode duckbox

-- 1. Verify baseline dataset metrics (lines.parquet)
SELECT 
    count(*) AS total_rows,
    round(41.97, 2) AS single_file_mib,
    count(DISTINCT date_trunc('month', created_at)) AS distinct_months,
    count(DISTINCT created_at::DATE) AS distinct_days,
    count(DISTINCT customer_id) AS distinct_customers,
    count(DISTINCT city) AS distinct_cities,
    count(DISTINCT category) AS distinct_categories,
    count(DISTINCT sku) AS distinct_skus
FROM 'data/lines.parquet';

-- 2. Count distinct candidate partition keys and calculate 1x and 100x sizing
CREATE OR REPLACE TEMP TABLE candidate_metrics AS
SELECT 'sale_month' AS candidate_key,
       count(DISTINCT date_trunc('month', created_at)) AS distinct_folders,
       'static' AS scaling_type
FROM 'data/lines.parquet'
UNION ALL
SELECT 'category', count(DISTINCT category), 'static' FROM 'data/lines.parquet'
UNION ALL
SELECT 'city', count(DISTINCT city), 'static' FROM 'data/lines.parquet'
UNION ALL
SELECT 'sale_day', count(DISTINCT created_at::DATE), 'static' FROM 'data/lines.parquet'
UNION ALL
SELECT 'sku', count(DISTINCT sku), 'static' FROM 'data/lines.parquet'
UNION ALL
SELECT 'customer_id', count(DISTINCT customer_id), 'proportional' FROM 'data/lines.parquet';

-- 3. Detailed arithmetic projection table (matches Section 2 of PDF deliverable)
SELECT 
    candidate_key,
    distinct_folders AS folders_1x,
    2000001 // distinct_folders AS rows_per_file_1x,
    round(41.97 * 1024 / distinct_folders, 1) AS est_kib_per_file_1x,
    CASE 
        WHEN scaling_type = 'static' THEN distinct_folders
        ELSE distinct_folders * 100
    END AS folders_100x,
    CASE 
        WHEN scaling_type = 'static' THEN 200000100 // distinct_folders
        ELSE 200000100 // (distinct_folders * 100)
    END AS rows_per_file_100x,
    CASE 
        WHEN scaling_type = 'static' THEN round((4.1 * 1024) / distinct_folders, 1)
        ELSE round((4.1 * 1024 * 1024) / (distinct_folders * 100), 2)
    END AS est_size_per_file_100x_unit
FROM candidate_metrics
ORDER BY folders_1x;

-- 4. Specific validation for our chosen partition key: sale_month
-- 100x Shop has 100 million orders (approx 200 million line items) across 3 years (36 calendar months)
SELECT 
    'sale_month (Chosen for Q1)' AS partition_key,
    36 AS total_folders_100x,
    round(4198.4 / 36, 2) AS mib_per_file_100x,
    'Target sweet spot is 100 MB - 512 MB per Parquet file (W5 Companion Section 7)' AS sizing_verdict;