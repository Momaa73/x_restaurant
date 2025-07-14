FROM bitnami/spark:3.5

USER root

# הורדת כל ה-JARs הנחוצים והעתקתם לתיקיית ה-jars של Spark
RUN mkdir -p /app/jars && \
    curl -L -o /app/jars/iceberg-spark-runtime.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.0/iceberg-spark-runtime-3.5_2.12-1.4.0.jar && \
    curl -L -o /app/jars/aws-bundle.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.0/iceberg-aws-bundle-1.4.0.jar && \
    curl -L -o /app/jars/spark-sql-kafka.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /app/jars/spark-token-provider-kafka.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /app/jars/kafka-clients.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /app/jars/commons-pool2.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    cp /app/jars/*.jar /opt/bitnami/spark/jars/

COPY create_tables_bronze.py /app/create_tables_bronze.py
COPY create_tables_silver.py /app/create_tables_silver.py
COPY create_tables_gold.py /app/create_tables_gold.py
COPY stream_to_bronze.py /app/stream_to_bronze.py
COPY bronze_to_silver.py /app/bronze_to_silver.py

WORKDIR /app
