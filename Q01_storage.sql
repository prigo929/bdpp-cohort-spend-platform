-- ==============================================================================
-- Checkpoint 1: Storage Layer Design — Question 1 (Q01)
-- Course: Big Data: Persistence Systems and Data Processing Languages
-- Lecturer: Ionuț Hrubaru, PhD
-- Team: Prigoreanu Alin-Gabriel, Bîrgovan A. Mihai-Daniel, Schipor Ana-Maria
--
-- Target Analytical Question (Q1):
--   "How does spend evolve for customers grouped by the month they first ordered?"
--
-- Purpose:
--   Computes distinct counts, estimated vs real file sizes, and 100x scale
--   projections (100 million orders, 4.1 GiB Parquet) for Checkpoint 1.
-- ==============================================================================

-- 1. Candidate Partition Key Counts over the provided dataset (data/lines.parquet)
-- Total lines: 2,000,001 | Single file size: 41.97 MiB (43,008 KiB)
SELECT 
    'category' AS candidate_key,
    count(DISTINCT category) AS folders_today,
    round(2000001 / count(DISTINCT category)) AS rows_per_file_today,
    round((41.97 * 1024) / count(DISTINCT category), 1) AS est_kib_today,
    count(DISTINCT category) AS folders_100x,
    round((4.1 * 1024) / count(DISTINCT category), 1) AS est_mib_100x
FROM 'data/lines.parquet'
UNION ALL
SELECT 
    'city',
    count(DISTINCT city),
    round(2000001 / count(DISTINCT city)),
    round((41.97 * 1024) / count(DISTINCT city), 1),
    count(DISTINCT city),
    round((4.1 * 1024) / count(DISTINCT city), 1)
FROM 'data/lines.parquet'
UNION ALL
SELECT 
    'month (sale_month)',
    count(DISTINCT date_trunc('month', created_at)::DATE),
    round(2000001 / count(DISTINCT date_trunc('month', created_at)::DATE)),
    round((41.97 * 1024) / count(DISTINCT date_trunc('month', created_at)::DATE), 1),
    count(DISTINCT date_trunc('month', created_at)::DATE),
    round((4.1 * 1024) / count(DISTINCT date_trunc('month', created_at)::DATE), 1)
FROM 'data/lines.parquet'
UNION ALL
SELECT 
    'day (sale_day)',
    count(DISTINCT created_at::DATE),
    round(2000001 / count(DISTINCT created_at::DATE)),
    round((41.97 * 1024) / count(DISTINCT created_at::DATE), 1),
    count(DISTINCT created_at::DATE),
    round((4.1 * 1024) / count(DISTINCT created_at::DATE), 1)
FROM 'data/lines.parquet'
UNION ALL
SELECT 
    'sku',
    count(DISTINCT sku),
    round(2000001 / count(DISTINCT sku)),
    round((41.97 * 1024) / count(DISTINCT sku), 1),
    count(DISTINCT sku),
    round((4.1 * 1024) / count(DISTINCT sku), 1)
FROM 'data/lines.parquet'
UNION ALL
SELECT 
    'customer_id',
    count(DISTINCT customer_id),
    round(2000001.0 / count(DISTINCT customer_id), 1),
    round((41.97 * 1024) / count(DISTINCT customer_id), 2),
    count(DISTINCT customer_id) * 100, -- customer count scales with 100x shop volume!
    0.00 -- file size remains ~0.4 KiB across 12M folders
FROM 'data/lines.parquet';


-- 2. Real vs Estimated Layout Comparison from W5 Lab (MinIO / S3)
-- In W5 step 3, writing lines partitioned by sale_month produced:
--   Real files: 36 objects
--   Real total size: 43.33 MiB (avg 1.20 MiB per partition)
--   Arithmetic estimate: 41.97 MiB / 36 = 1.17 MiB per partition
--   Difference: +3.2% overhead due to 36 independent Parquet footers and dictionary pages.
--
-- In W5 step 5, writing lines partitioned by sale_day produced:
--   Real files: 1,096 objects
--   Real total size: 59.40 MiB (avg 54.2 KiB per partition)
--   Arithmetic estimate: 41.97 MiB / 1096 = 39.2 KiB
--   Difference: +41.5% overhead due to 1,096 repeated schemas, footers, and index blocks.


-- 3. Q1 Specific Projections for 100x Shop (100 Million Orders, ~4.1 GiB Parquet)
-- Timeline: 3 years = 36 months (fixed time axis; months do not grow with shop volume).
SELECT 
    '100x Shop Plan (Q1)' AS scope,
    36 AS partition_folders,
    round(200000000 / 36) AS rows_per_partition, -- 200M line items across 100M orders
    round((4.1 * 1024) / 36, 1) AS mib_per_partition_file, -- ~116.6 to 120.4 MiB
    'Matches W5 Section 7 guidance (100 MiB - 512 MiB optimal range)' AS file_size_evaluation;


-- 4. Ingestion Feed Verification (data/refunds.csv)
-- Late refunds arriving up to 40 days late:
SELECT 
    count(*) AS total_refund_records,
    round(count(*) * 100) AS refunds_100x_shop,
    min(refunded_at) AS earliest_refund,
    max(refunded_at) AS latest_refund
FROM 'data/refunds.csv';
