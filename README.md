# Customer Cohort Spend Evolution (Q1 Mini Data Platform)

Academic project for the **Big Data: Persistence Systems and Data Processing Languages** course (Master SDBIS, FEAA, Alexandru Ioan Cuza University of Iași).

This platform answers Question 1 from Annex A:
> *How does spend evolve for customers grouped by the month they first ordered?*

---

## 1. Project Team

* **Alin-Gabriel Prigoreanu** (Architecture design, DuckDB pipeline, dbt modeling)
* [Team Member 2] (PostgreSQL ingestion, MinIO extraction)
* [Team Member 3] (Data quality testing, dimensional modeling)

---

## 2. Platform Architecture

The pipeline processes data sequentially, running at most two containers at a time:

```text
[ PostgreSQL Source ]
         │
         ▼  (extract/extract_to_minio.py)
[ MinIO Raw Bucket ] ── s3://lab/raw/
         │
         ▼  (scripts/partition_storage.sql)
[ Partitioned Parquet ] ── s3://lab/by_month/sale_month=YYYY-MM-01/
         │
         ▼  (dbt-duckdb)
[ Dimensional Layer / Marts ]
   ├── staging: stg_orders, stg_lines, stg_refunds
   ├── intermediate: int_cohort_customers, int_date_spine
   └── marts: fct_cohort_monthly_spend
         │
         ▼
[ Analytical Result: Q1 Cohort Matrix ]
```

---

## 3. Repository Structure

```text
├── .gitignore
├── README.md
├── docker-compose.yml
├── extract/
│   ├── extract_to_minio.py
│   └── requirements.txt
├── scripts/
│   └── partition_storage.sql
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_lines.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_refunds.sql
│   │   ├── intermediate/
│   │   │   ├── int_cohort_customers.sql
│   │   │   └── int_date_spine.sql
│   │   └── marts/
│   │       ├── fct_cohort_monthly_spend.sql
│   │       └── schema.yml
│   └── tests/
│       └── assert_non_negative_spend.sql
├── analysis/
│   └── q1_cohort_spend_evolution.sql
└── docs/
    ├── checkpoint1_storage_design.md
    └── design_document.pdf
```

---

## 4. Prerequisites

* Docker Desktop installed and running.
* MinIO Docker images loaded from the Week 1 package (`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` and `quay.io/minio/mc:latest`).
* Git installed.

---

## 5. End-to-End Reproduction Guide

Run the following commands in order from your terminal to recreate the platform and verify the analytical answer.

### Step 1: Clone the Repository

```bash
git clone [https://github.com/](https://github.com/)[your-username]/bdpp-cohort-spend-platform.git
cd bdpp-cohort-spend-platform
```

### Step 2: Start the Containers

This command builds the lab container and starts MinIO alongside PostgreSQL:

```bash
docker compose up -d --wait
```

Verify that all services report healthy status:

```bash
docker compose ps -a
```

### Step 3: Ingest Raw Data into MinIO

Extract data from PostgreSQL and place raw extracts into the MinIO `lab` bucket:

```bash
docker compose exec lab python extract/extract_to_minio.py
```

### Step 4: Write Partitioned Storage Layout

Use DuckDB to partition the raw dataset by month (`sale_month=YYYY-MM-01`) on object storage:

```bash
docker compose exec lab duckdb -c ".read scripts/partition_storage.sql"
```

Verify the written partitions in MinIO:

```bash
docker compose exec lab duckdb -c "
LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false);
SELECT split_part(filename, '/', 4) AS prefix, count(*) AS objects FROM read_blob('s3://lab/**') GROUP BY 1;
"
```

### Step 5: Execute dbt Transformations and Data Tests

Run dbt models to build staging views, cohort dimensions, date spines, and fact tables:

```bash
docker compose exec lab bash -c "cd dbt_project && dbt run"
```

Run dbt tests to guarantee unique keys, non-null values, and referential integrity:

```bash
docker compose exec lab bash -c "cd dbt_project && dbt test"
```

### Step 6: Produce Final Analytical Output for Q1

Execute the final cohort query against the materialized fact table:

```bash
docker compose exec lab duckdb -c ".read analysis/q1_cohort_spend_evolution.sql"
```

---

## 6. Architectural Decisions for Question 1

> 📖 **Full Checkpoint 1 (Week 6) Defense Document:** See [docs/checkpoint1_storage_design.md](docs/checkpoint1_storage_design.md) for the complete storage architecture diagram, detailed justifications, and oral evaluation defense questions.

* **Storage Format (Parquet):** We store analytical datasets in columnar Parquet format. We rejected raw CSV and JSON lines because cohort reporting requires reading only four specific columns (`customer_id`, `created_at`, `quantity`, `unit_price`). Columnar projection eliminates scanning unused transaction attributes.
* **Partition Key (`sale_month`):** We partition raw Parquet files by transaction month. We rejected partitioning by `customer_id` because 119,975 distinct values produce 119,975 tiny files of 16 lines each, causing extreme metadata request overhead. We rejected an unpartitioned single file because every monthly report would scan all historical years.
* **Complete Date Spine:** Customers do not purchase in every consecutive month. We generate a complete month sequence using `generate_series`. Months without orders receive explicit zero values (`0.00`) via left joins, preventing missing periods from breaking cohort retention metrics.
* **Late-Arriving Refunds:** The payment provider delivers refunds up to 40 days late. In our fact table, refunds update net revenue historically against the order placement month rather than the refund arrival date, keeping cohort spend comparisons consistent.

---

## 7. Teardown

To stop active containers while keeping data volumes intact:

```bash
docker compose stop
```

To remove all containers and delete local data volumes:

```bash
docker compose down -v
```
