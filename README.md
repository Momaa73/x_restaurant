# Restaurant Data Engineering Project

## Overview
This project is a complete end-to-end data engineering solution for a restaurant analytics platform. It demonstrates modern data engineering practices using Apache Iceberg, MinIO, Apache Spark, Apache Kafka, and Apache Airflow, all orchestrated via Docker Compose. The pipeline ingests, processes, and models both batch and streaming data, supporting robust analytics and reporting.

## Architecture
- **Data Storage:** MinIO (S3-compatible) with Apache Iceberg tables, organized in bronze, silver, and gold layers.
- **Processing:** Apache Spark for batch and streaming ETL jobs.
- **Streaming:** Apache Kafka for real-time data ingestion; Python producer for generating messages.
- **Orchestration:** Apache Airflow for scheduling and managing ETL pipelines.

See the [full architecture diagram](docs/architecture.md) for a visual overview.

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

## Sample Batch Data
Sample batch data files are provided for initial loading into the bronze Iceberg tables. You can find them in `processing/demo_data/`:
- `reservations.csv`
- `checkins.csv`
- `feedback.csv`

### Loading Batch Data
To load the sample batch data into the bronze layer, you can use a Spark job or manually load them using Spark SQL. For example, you can add a step in your Airflow DAG or run the following Spark code:

```python
# Example: Load reservations.csv into the bronze table
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("/app/processing/demo_data/reservations.csv", header=True, inferSchema=True)
df.write.format("iceberg").mode("append").save("my_catalog.bronze.Reservations_raw")
```
Repeat for `checkins.csv` and `feedback.csv` as needed.

## Component Descriptions
### /orchestration
- Contains Airflow DAGs and configuration for orchestrating the ETL pipeline. Airflow schedules and manages the end-to-end data flow, triggering Spark jobs and monitoring pipeline health.

### /streaming
- Contains Kafka setup and a Python producer for generating real-time messages. Simulates real-time data ingestion by sending demo events to Kafka topics.

### /processing
- Contains Spark batch and streaming jobs for ETL, SCD2, and data quality. All data transformations, cleaning, and enrichment logic are implemented here. Spark jobs read/write Iceberg tables on MinIO.

### /docs
- Contains data model diagrams (in Mermaid.js) and architecture documentation. Explains the structure and relationships of all tables in the bronze, silver, and gold layers.

---

## Data Quality
Data quality checks are implemented in Spark jobs, primarily in `processing/spark/bronze_to_silver.py`.

- **Null Checks:** Required columns for each entity (reservations, checkins, feedback) are checked for nulls. If any are found, the count and a sample are printed in the Spark job logs.
- **Duplicate Checks:** Checks for duplicate primary keys (e.g., reservation_id + created_at_date). Duplicates are reported in the logs.
- **Referential Integrity Checks:** Ensures all foreign keys (e.g., branch_id, table_id, phone_number) exist in the corresponding parent tables. Violations are reported in the logs.

**How to View Results:**
- Run the pipeline via Airflow or directly with Spark jobs. Data quality issues will be printed in the Spark job logs (viewable via Docker logs or Spark UI).

---

## How to Test Late-Arriving Data
The pipeline is designed to handle late-arriving data (up to 48 hours after event time) using Spark watermarking and upsert logic.

**To test:**
1. Edit a message in `streaming/messages/` (e.g., `reservations.json`) and set the event time (e.g., `created_at` or `reservation_date`) to a value within the last 48 hours, but after the initial pipeline run.
2. Re-run the Python producer to send the message to Kafka.
3. Trigger the Airflow DAG or run the Spark streaming job again.
4. The new/late event will be ingested and upserted into the bronze and silver tables, updating the analytics as needed.

---

## How to Inspect Iceberg Tables
You can inspect the contents of Iceberg tables using Spark SQL or the Spark shell.

**Option 1: Spark Shell (inside the processing container):**
```bash
docker exec -it spark-iceberg spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0
```
Then run:
```sql
SELECT * FROM my_catalog.bronze.Reservations_raw LIMIT 10;
SELECT * FROM my_catalog.silver.reservations_cleaned LIMIT 10;
SELECT * FROM my_catalog.gold.fact_reservations LIMIT 10;
```
**Option 2: Use a notebook:**
- Start a Jupyter notebook (if available) in the processing container and use PySpark to query tables.

**Option 3: MinIO UI:**
- Browse the raw Parquet files in MinIO at http://localhost:9000 (user: admin, password: password). Note: This shows files, not table structure.

## SCD2 (Slowly Changing Dimension Type 2)
- The pipeline implements SCD2 logic for the `branch` dimension in the silver layer.
- See `processing/spark/silver_to_gold.py` for implementation details.

### SCD2 Implementation Details
- The `scd2_branch` table tracks the full history of changes to branch attributes (e.g., name, address, city, capacity).
- When a branch attribute changes, a new row is inserted with the new values, and the previous row is marked as no longer current (using an `is_update` flag or end date).
- This allows you to answer questions like "What was the branch address at a given time?" and ensures historical accuracy in analytics.
- The ETL logic ensures that fact tables always join to the correct version of the branch record based on event time.

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
- Eden Adiv
- Kori Zohar

## License
MIT License 

---

## How to Reset the Environment
If you want to start from a clean slate (remove all data, containers, and volumes):

1. **Stop all running containers:**
   ```bash
   docker-compose down -v
   # Run in each of the orchestration, processing, and streaming folders
   ```
2. **Remove all Docker volumes (optional, will delete all data!):**
   ```bash
   docker volume prune -f
   ```
3. **Delete any leftover local data directories (e.g., warehouse, minio_data):**
   ```bash
   rm -rf processing/warehouse orchestration/minio_data
   ```
4. **Rebuild images (if needed):**
   ```bash
   docker-compose build
   ```
5. **Restart all services as described below.**

---

## How to Run a Full End-to-End Demo
Follow these steps to demonstrate the entire pipeline from data generation to analytics-ready tables:

1. **Start all services:**
   - In three terminals, run:
     ```bash
     cd streaming && docker-compose up -d
     cd ../processing && docker-compose up -d
     cd ../orchestration && docker-compose up -d
     ```
2. **Generate and send demo data:**
   - In the `streaming` folder, run:
     ```bash
     python producer.py
     ```
   - This will send demo reservations, checkins, and feedback to Kafka.
3. **Trigger the Airflow DAG:**
   - Open Airflow UI at [http://localhost:8082](http://localhost:8082)
   - Find the `restaurant_pipeline` DAG and trigger it manually (or wait for the schedule).
4. **Monitor pipeline progress:**
   - Use Airflow UI to see task status.
   - Use Spark UI (http://localhost:4040, when jobs are running) to monitor Spark jobs.
   - Use MinIO UI (http://localhost:9000, user: admin, password: password) to browse data files.
5. **Inspect results:**
   - Use Spark SQL or a notebook to query Iceberg tables as described above.
6. **Test late-arriving data:**
   - Edit a message in `streaming/messages/` with a recent timestamp and re-run the producer, then re-trigger the DAG.

--- 