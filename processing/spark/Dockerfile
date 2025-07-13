FROM bitnami/spark:3.5

USER root

RUN mkdir -p /app/jars && \
    curl -L -o /app/jars/iceberg-spark-runtime.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    curl -L -o /app/jars/aws-bundle.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar && \
    cp /app/jars/*.jar /opt/bitnami/spark/jars/

COPY create_tables_bronze.py /app/create_tables_bronze.py
COPY create_tables_silver.py /app/create_tables_silver.py
# COPY bronze_to_silver.py /app/bronze_to_silver.py

WORKDIR /app
