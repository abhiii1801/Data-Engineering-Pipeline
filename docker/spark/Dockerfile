FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install \
    azure-storage-file-datalake \
    delta-spark==3.1.0 \
    great-expectations \
    azure-storage-blob \
    azure-identity \
    python-dotenv \
    faker \
    pandas

# NEW: Pre-download Delta Lake JARs to the Spark jars directory
# This prevents Spark from needing to download them at runtime
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar -o /opt/spark/jars/delta-storage-3.1.0.jar

ENV PYSPARK_PYTHON=python3

USER spark