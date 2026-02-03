# Data Engineering Pipeline (Airflow + Spark + ADLS + Azure SQL)

An end-to-end batch pipeline that generates dummy sales data, ingests it into **Azure Data Lake Storage Gen2 (ADLS)**, transforms it with **PySpark + Delta Lake**, runs **data quality checks**, and loads a **fact table** into **Azure SQL** for reporting (e.g., a Power BI dashboard built on `dbo.fact_sales`).

## Architecture (daily)

1. **Create dummy data** → writes `data/YYYY/MM/DD/sales_raw.csv`
2. **Ingest to ADLS** → uploads CSV to `raw/sales/YYYY/MM/DD/sales_raw.csv`
3. **Spark ETL** → reads raw CSV, enriches + derives measures, writes **Delta** to `processed/sales/` (partitioned)
4. **Data quality checks** → validates processed Delta data
5. **Load to Azure SQL** → writes `dbo.fact_sales` (source for Power BI)

Orchestrated by **Apache Airflow**; Spark runs in a Docker container and is triggered from Airflow via `docker exec`.

## Repo layout

- `dags/sales_pipeline_dag.py` — Airflow DAG (`sales_data_pipeline`)
- `data/data_creation.py` — dummy data generator (creates partitioned CSVs under `data/`)
- `ingestion/upload_to_adls.py` — uploads CSV to ADLS Gen2 (`raw` filesystem)
- `spark_jobs/sales_etl.py` — PySpark ETL → Delta write to ADLS Gen2 (`processed` filesystem)
- `data_quality/expectations.py` — PySpark data quality checks on processed Delta
- `spark_jobs/load_to_sql.py` — loads curated data into Azure SQL (`dbo.fact_sales`)
- `sql/create_tables.sql` — starter DDL for the fact table (adjust types if needed)
- `docker/docker-compose.yml` — local runtime (Airflow + Spark containers)
- `docker/airflow/Dockerfile`, `docker/spark/Dockerfile` — container images

## Prerequisites

- Docker Desktop (Linux containers; WSL2 recommended on Windows)
- An **ADLS Gen2** Storage Account with **file systems/containers**:
  - `raw`
  - `processed`
- An **Azure SQL** database with a `dbo.fact_sales` table

## Configuration

Create a `.env` file in the project root (do **not** commit real secrets). Required variables:

```env
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_account_key

AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DB=your_database
AZURE_SQL_USER=your_username
AZURE_SQL_PASSWORD=your_password
```

## Run locally (Airflow + Spark in Docker)

Start the stack:

```bash
docker compose -f docker/docker-compose.yml up --build
```

Open Airflow:

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

Trigger the DAG:

- DAG id: `sales_data_pipeline`
- Schedule: `@daily`
- Manual run with a specific date: trigger with config like:
  ```json
  { "date": "2026-01-27" }
  ```

## Data model (fact table)

The SQL load job writes to `dbo.fact_sales` from the curated/processed dataset. Columns written by the Spark load step:

- `order_id`, `product_id`, `customer_id`
- `quantity`, `price`, `total_amount`, `price_bucket`
- `order_date`, `order_year`, `order_month`, `processing_date`

Note: IDs produced by the dummy generator are string-shaped (e.g., `ORD000000123`, `PROD100001`). Ensure your Azure SQL schema uses compatible types (e.g., `VARCHAR`) before running the load.

## Power BI

Connect Power BI to Azure SQL and build visuals on top of `dbo.fact_sales` (measures like `SUM(total_amount)`, trends by `order_year/order_month`, `price_bucket`, `category` if included, etc.).

