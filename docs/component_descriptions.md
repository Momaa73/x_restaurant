# Component Descriptions

This document describes each component in the restaurant data pipeline architecture, their purposes, configurations, and roles in the overall system.

## Overview

The restaurant data pipeline uses a modern data stack with the following components:
- **Apache Spark**: Distributed data processing
- **Apache Kafka**: Real-time data streaming
- **Apache Airflow**: Workflow orchestration
- **Apache Iceberg**: Table format and ACID transactions
- **MinIO**: S3-compatible object storage
- **Great Expectations**: Data quality validation
- **Docker**: Containerization and deployment

---

## Apache Spark

### Purpose
Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general computation graphs.

### Role in Pipeline
- **Batch Processing**: Transforms data between Bronze, Silver, and Gold layers
- **Streaming**: Processes real-time data from Kafka
- **SQL Queries**: Enables SQL operations on structured data
- **Data Validation**: Integrates with Great Expectations for data quality checks

### Configuration
```python
# Key Spark configurations
spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.type=hadoop
spark.sql.catalog.my_catalog.warehouse=s3a://warehouse/
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=admin
spark.hadoop.fs.s3a.secret.key=password
spark.hadoop.fs.s3a.path.style.access=true
```

### Key Features
- **Distributed Computing**: Processes data across multiple nodes
- **In-Memory Processing**: Faster than traditional disk-based processing
- **Fault Tolerance**: Automatically recovers from node failures
- **Streaming Support**: Real-time data processing with structured streaming

---

## Apache Kafka

### Purpose
Apache Kafka is a distributed streaming platform that enables building real-time data pipelines and streaming applications.

### Role in Pipeline
- **Data Ingestion**: Receives data from various sources (reservations, checkins, feedback)
- **Message Queue**: Buffers data between producers and consumers
- **Real-time Streaming**: Enables real-time data processing
- **Data Distribution**: Distributes data to multiple consumers

### Topics
- **reservations**: Restaurant reservation data
- **checkins**: Customer check-in events
- **feedback**: Customer feedback and ratings

### Configuration
```yaml
# Kafka configuration in docker-compose.yml
kafka:
  image: confluentinc/cp-kafka:latest
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### Key Features
- **High Throughput**: Can handle millions of messages per second
- **Scalability**: Horizontally scalable across multiple brokers
- **Durability**: Messages are persisted to disk
- **Real-time Processing**: Enables real-time data pipelines

---

## Apache Airflow

### Purpose
Apache Airflow is a platform to programmatically author, schedule, and monitor workflows. It allows you to define complex workflows as code.

### Role in Pipeline
- **Workflow Orchestration**: Coordinates the entire data pipeline
- **Scheduling**: Runs jobs at specified intervals
- **Monitoring**: Tracks job success/failure
- **Dependency Management**: Ensures proper execution order
- **Error Handling**: Manages retries and alerts

### DAG Structure
```python
# Main pipeline DAG
restaurant_pipeline:
  - generate_data_task: Creates sample data
  - kafka_producer_task: Sends data to Kafka
  - spark_streaming_task: Processes streaming data
  - spark_batch_task: Runs batch transformations
  - data_quality_task: Validates data quality
```

### Configuration
```python
# Airflow configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

### Key Features
- **DAG-based Workflows**: Directed Acyclic Graphs for workflow definition
- **Rich UI**: Web interface for monitoring and managing workflows
- **Extensible**: Plugin system for custom operators
- **Scalable**: Can handle thousands of concurrent tasks

---

## Apache Iceberg

### Purpose
Apache Iceberg is a table format for large analytic datasets. It provides ACID transactions, schema evolution, and time travel capabilities.

### Role in Pipeline
- **Table Format**: Defines how data is stored and organized
- **ACID Transactions**: Ensures data consistency
- **Schema Evolution**: Allows schema changes without breaking existing queries
- **Time Travel**: Enables querying data at specific points in time
- **Partitioning**: Efficient data organization and querying

### Table Structure
```sql
-- Example Iceberg table creation
CREATE TABLE my_catalog.bronze.reservations (
    reservation_id STRING,
    customer_id STRING,
    reservation_time TIMESTAMP,
    holiday STRING,
    raw_notes STRING
) USING iceberg
PARTITIONED BY (date(reservation_time));
```

### Key Features
- **ACID Transactions**: Atomic, consistent, isolated, and durable operations
- **Schema Evolution**: Add, remove, or modify columns safely
- **Hidden Partitioning**: Automatic partition management
- **Time Travel**: Query data at specific snapshots
- **Compaction**: Automatic file optimization

---

## MinIO

### Purpose
MinIO is a high-performance, S3-compatible object storage server. It provides scalable, secure, and cost-effective storage for unstructured data.

### Role in Pipeline
- **Data Storage**: Stores all data files (Parquet, Avro, etc.)
- **S3 Compatibility**: Provides S3 API for easy integration
- **Scalability**: Handles large amounts of data
- **Security**: Access control and encryption

### Configuration
```yaml
# MinIO configuration
minio:
  image: minio/minio:latest
  environment:
    MINIO_ROOT_USER: admin
    MINIO_ROOT_PASSWORD: password
  command: server /data --console-address ":9001"
  ports:
    - "9000:9000"
    - "9001:9001"
```

### Bucket Structure
```
warehouse/
├── bronze/
│   ├── reservations/
│   ├── checkins/
│   └── feedback/
├── silver/
│   ├── reservations_cleaned/
│   ├── checkins_cleaned/
│   └── feedback_cleaned/
└── gold/
    ├── fact_reservations/
    ├── dim_customers/
    └── metrics/
```

### Key Features
- **S3 Compatible**: Works with existing S3 tools and libraries
- **High Performance**: Optimized for high-throughput workloads
- **Scalable**: Can scale to petabytes of data
- **Secure**: Encryption at rest and in transit

---

## Great Expectations

### Purpose
Great Expectations is a Python library that helps data teams eliminate pipeline debt by testing, documenting, and profiling data to maintain quality.

### Role in Pipeline
- **Data Validation**: Ensures data meets quality standards
- **Testing**: Automated tests for data quality
- **Documentation**: Generates data quality reports
- **Monitoring**: Tracks data quality over time
- **Alerting**: Notifies when data quality issues are detected

### Validation Types
```python
# Example expectations
expect_column_values_to_not_be_null("reservation_id")
expect_column_values_to_be_between("rating", 1, 5)
expect_table_row_count_to_be_between(100, 10000)
expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
```

### Configuration
```python
# Great Expectations configuration
context = ge.get_context()
datasource_config = {
    "name": "spark_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        }
    }
}
```

### Key Features
- **Declarative Expectations**: Define data quality rules in simple language
- **Automated Testing**: Run validations as part of data pipelines
- **Rich Documentation**: Generate detailed data quality reports
- **Integration**: Works with Airflow, Spark, and other tools

---

## Docker

### Purpose
Docker is a platform for developing, shipping, and running applications in containers. It provides consistency across different environments.

### Role in Pipeline
- **Containerization**: Packages applications and dependencies
- **Environment Consistency**: Ensures same behavior across environments
- **Easy Deployment**: Simplifies deployment and scaling
- **Isolation**: Separates different services and their dependencies

### Services
```yaml
# Docker Compose services
services:
  spark-iceberg:
    image: custom-spark-iceberg
    ports:
      - "4040:4040"
  
  kafka:
    image: confluentinc/cp-kafka:latest
  
  airflow:
    image: apache/airflow:latest
  
  minio:
    image: minio/minio:latest
```

### Key Features
- **Portability**: Run anywhere Docker is installed
- **Isolation**: Each container runs independently
- **Efficiency**: Lightweight compared to virtual machines
- **Versioning**: Easy to manage different versions of services

---

## Data Flow Summary

```
Data Sources → Kafka → Spark Streaming → Bronze Layer (Iceberg)
                                    ↓
                              Spark Batch → Silver Layer (Iceberg)
                                    ↓
                              Spark Batch → Gold Layer (Iceberg)
                                    ↓
                              Great Expectations → Data Quality Reports
```

**Airflow orchestrates the entire pipeline, ensuring proper execution order and handling failures.**

---

## Performance Considerations

### Spark
- **Memory Configuration**: Adjust based on data size
- **Partitioning**: Optimize for query patterns
- **Caching**: Cache frequently accessed data

### Kafka
- **Topic Partitioning**: Distribute load across partitions
- **Replication Factor**: Ensure data durability
- **Retention Policy**: Manage storage costs

### Iceberg
- **File Size**: Optimize for read performance
- **Partitioning Strategy**: Align with query patterns
- **Compaction**: Regular maintenance for performance

### MinIO
- **Storage Class**: Choose appropriate storage tier
- **Lifecycle Policies**: Automate data management
- **Access Patterns**: Optimize for read/write patterns

---

## Security Considerations

- **Authentication**: Secure access to all components
- **Authorization**: Role-based access control
- **Encryption**: Data encryption at rest and in transit
- **Network Security**: Secure communication between services
- **Audit Logging**: Track access and changes

---

## Monitoring and Alerting

- **Health Checks**: Monitor service availability
- **Performance Metrics**: Track response times and throughput
- **Error Rates**: Monitor failure rates and types
- **Resource Usage**: Track CPU, memory, and storage usage
- **Data Quality**: Monitor validation results and trends
