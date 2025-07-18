# Restaurant Data Engineering Project

## Overview
This project is a complete end-to-end data engineering solution for a restaurant analytics platform. It demonstrates modern data engineering practices using Apache Iceberg, MinIO, Apache Spark, Apache Kafka, and Apache Airflow, all orchestrated via Docker Compose. The pipeline ingests, processes, and models both batch and streaming data, supporting robust analytics and reporting.

## Architecture
- **Data Storage:** MinIO (S3-compatible) with Apache Iceberg tables, organized in bronze, silver, and gold layers.
- **Processing:** Apache Spark for batch and streaming ETL jobs.
- **Streaming:** Apache Kafka for real-time data ingestion; Python producer for generating messages.
- **Orchestration:** Apache Airflow for scheduling and managing ETL pipelines.

```
          +-------------------+
          |   Data Producer   |
          +--------+----------+
                   |
                   v
          +-------------------+
          |      Kafka        |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Spark Streaming |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Bronze Layer    |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Spark Batch     |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Silver Layer    |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Spark Batch     |
          +--------+----------+
                   |
                   v
          +-------------------+
          |   Gold Layer      |
          +-------------------+
                   |
                   v
          +-------------------+
          |   Analytics/BI    |
          +-------------------+
```

## Project Structure
```
/project-root
├── orchestration/        # Airflow components
├── streaming/            # Kafka and producers
├── processing/           # Spark applications
├── docs/                 # Project documentation and data model diagrams
├── README.md             # This file
```

## Data Models
- [Bronze Layer Data Model](docs/bronze_data_model.md)
- [Silver Layer Data Model](docs/silver_data_model.md)
- [Gold Layer Data Model](docs/gold_data_model.md)

## Setup Instructions
### Prerequisites
- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- Python 3.8+ (for the Kafka producer)

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd x_restaurant
```

### 2. Start Each Component
Each component can be started independently for development, or all together for a full pipeline demo.

#### Start Streaming (Kafka + Producer)
```bash
cd streaming
# Start Kafka and Zookeeper
docker-compose up -d
# In a new terminal, run the Python producer
python producer.py
```

#### Start Processing (Spark + MinIO)
```bash
cd ../processing
# Start Spark and MinIO
docker-compose up -d
```

#### Start Orchestration (Airflow)
```bash
cd ../orchestration
# Start Airflow
docker-compose up -d
```

### 3. Access Services
- **Airflow UI:** http://localhost:8082A
- **MinIO UI:** http://localhost:9000 (user: admin, password: password)
- **Spark UI:** http://localhost:4040 (when Spark job is running)
- **Kafka UI:** (if included)

### 4. Run the Pipeline
- Trigger the Airflow DAG (`restaurant_pipeline`) from the Airflow UI.
- The pipeline will:
  - Ingest streaming and batch data into the bronze layer
  - Transform data to silver and gold layers
  - Run data quality checks

### 5. Inspect Data
- Use MinIO UI to browse Iceberg table files.
- Use Spark SQL or Trino to query Iceberg tables (optional).

## Component Descriptions
### /orchestration
- Contains Airflow DAGs and configuration for orchestrating the ETL pipeline.

### /streaming
- Contains Kafka setup and a Python producer for generating real-time messages.

### /processing
- Contains Spark batch and streaming jobs for ETL, SCD2, and data quality.

### /docs
- Contains data model diagrams and architecture documentation.

## Data Quality
- Data quality checks are implemented in Spark jobs and/or as Airflow tasks.
- Checks include: null value checks, duplicate detection, referential integrity.
- (Bonus) [Great Expectations](https://greatexpectations.io/) integration for advanced validation (if implemented).

## SCD2 (Slowly Changing Dimension Type 2)
- The pipeline implements SCD2 logic for the `branch` dimension in the silver layer.
- See `processing/spark/silver_to_gold.py` for implementation details.

## Late-Arriving Data
- The pipeline handles late-arriving data (up to 48 hours) using Spark windowing and upsert logic.
- See Spark job code for details.

## Bonus Features
- **Data Quality with Great Expectations:** (if implemented)
- **Data Lineage with DataHub:** (if implemented)

## Troubleshooting
- Ensure all Docker containers are running (`docker ps`).
- Check Airflow, Spark, and MinIO logs for errors.
- If ports are in use, change them in the respective `docker-compose.yml` files.

## Authors
- Moran Benyamin

## License
MIT License 