# Data Engineering Pipeline (Airflow + Spark + ADLS + Azure SQL + Power BI)

An end-to-end **batch data engineering pipeline** that generates dummy sales data, ingests it into **Azure Data Lake Storage Gen2 (ADLS)**, transforms it using **PySpark + Delta Lake**, performs **data quality checks**, and loads a curated **fact table** into **Azure SQL**, which is then consumed by **Power BI** for reporting and analytics.

---

## Architecture (Daily Batch)

1. **Data Generation**  
   Generates dummy sales data and writes CSV files to  
   `data/YYYY/MM/DD/sales_raw.csv`

2. **Ingestion to ADLS Gen2**  
   Uploads raw CSV files to  
   `raw/sales/YYYY/MM/DD/sales_raw.csv`

3. **Spark ETL (PySpark + Delta Lake)**  
   - Reads raw data from ADLS  
   - Cleans, enriches, and derives business metrics  
   - Writes curated data as **Delta tables** to  
     `processed/sales/` (partitioned by date)

4. **Data Quality Checks**  
   Validates processed Delta data (null checks, schema validation, business rules)

5. **Load to Azure SQL**  
   Loads curated data into `dbo.fact_sales`

6. **Power BI Reporting**  
   Power BI connects to **Azure SQL** and builds dashboards and reports on top of  
   `dbo.fact_sales`

All steps are orchestrated using **Apache Airflow**. Spark jobs run inside Docker containers and are triggered from Airflow via `docker exec`.

---

## End-to-End Flow

```
Dummy Data
   ↓
Local CSV
   ↓
ADLS Gen2 (raw)
   ↓
Spark ETL + Delta Lake
   ↓
ADLS Gen2 (processed)
   ↓
Data Quality Checks
   ↓
Azure SQL (dbo.fact_sales)
   ↓
Power BI Dashboards & Reports
```

---

## Repository Structure

```
├── dags/
│   └── sales_pipeline_dag.py
├── data/
│   └── data_creation.py
├── ingestion/
│   └── upload_to_adls.py
├── spark_jobs/
│   ├── sales_etl.py
│   └── load_to_sql.py
├── data_quality/
│   └── expectations.py
├── sql/
│   └── create_tables.sql
├── docker/
│   ├── docker-compose.yml
│   ├── airflow/Dockerfile
│   └── spark/Dockerfile
└── .env
```

---

## Prerequisites

- Docker Desktop (Linux containers, WSL2 recommended on Windows)
- Azure Data Lake Storage Gen2 (`raw`, `processed`)
- Azure SQL Database
- Power BI Desktop

---

## Configuration

Create a `.env` file in the project root:

```env
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_account_key

AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DB=your_database
AZURE_SQL_USER=your_username
AZURE_SQL_PASSWORD=your_password
```

---

## Run Locally

```bash
docker compose -f docker/docker-compose.yml up --build
```

- Airflow UI: http://localhost:8080  
- Login: admin / admin  
- Trigger DAG: `sales_data_pipeline`

---

## Data Model – dbo.fact_sales

- order_id  
- product_id  
- customer_id  
- quantity  
- price  
- total_amount  
- price_bucket  
- order_date  
- order_year  
- order_month  
- processing_date  

---
## Power BI

Power BI connects directly to **Azure SQL** and uses `dbo.fact_sales` as the semantic layer for:

- Sales trends
- Revenue analysis
- Product and customer insights
- Time-based aggregations

This completes a full **data engineering → analytics** workflow.
