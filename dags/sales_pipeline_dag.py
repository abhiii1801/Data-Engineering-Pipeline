from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------------------------------------------------
# DYNAMIC DATE CONFIGURATION
# We use Jinja templating to grab the date.
# If triggered manually with {"date": "2026-01-27"}, it uses that.
# Otherwise, it defaults to the Airflow logical date (ds).
# -----------------------------------------------------------------------
DATE_VAR = "{{ dag_run.conf.get('date', ds) }}"

with DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='End-to-end Sales Pipeline triggering Spark in Docker',
    schedule_interval='@daily',  # Runs once a day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'etl', 'sales'],
) as dag:

    # 1. Data Creation (Running inside spark-etl container to access /opt/project)
    # Note: We assume data_creation.py writes to /opt/project/data/...
    create_data = BashOperator(
        task_id='create_dummy_data',
        bash_command=f"docker exec spark-etl python3 /opt/project/data/data_creation.py {DATE_VAR}"
    )

    # 2. Ingestion (Upload CSV to ADLS)
    ingest_to_adls = BashOperator(
        task_id='ingest_to_adls',
        bash_command=f"docker exec spark-etl python3 /opt/project/ingestion/upload_to_adls.py {DATE_VAR}"
    )

    # 3. Spark ETL (Read ADLS -> Transform -> Write Delta)
    spark_etl_job = BashOperator(
        task_id='spark_transform_job',
        bash_command=(
            "docker exec spark-etl spark-submit "
            "--conf spark.jars.ivy=/opt/project/.ivy2 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4,com.azure:azure-storage-blob:12.25.0 "
            "/opt/project/spark_jobs/sales_etl.py {{ ds }}"
        )
    )

    # 4. Data Quality Checks
    data_quality_checks = BashOperator(
        task_id='data_quality_checks',
        bash_command=(
            f"docker exec spark-etl spark-submit "
            f"--conf spark.jars.ivy=/tmp/ivy "
            f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            f"--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            f"--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4 "
            f"/opt/project/data_quality/expectations.py {DATE_VAR}"
        )
    )

    # 5. Load to Azure SQL (Fact Table)
    load_to_sql = BashOperator(
        task_id='load_to_azure_sql',
        bash_command=(
            f"docker exec spark-etl spark-submit "
            f"--conf spark.jars.ivy=/tmp/ivy "
            f"--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 "
            f"/opt/project/spark_jobs/load_to_sql.py {DATE_VAR}"
        )
    )

    # Define Dependencies
    create_data >> ingest_to_adls >> spark_etl_job >> data_quality_checks >> load_to_sql