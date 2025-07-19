# 🍽️ **Restaurant Data Engineering Project**

---

## **Project Overview**
A modern, end-to-end data engineering solution for restaurant analytics, featuring:
- **Apache Iceberg** for table format
- **MinIO** (S3-compatible) for storage
- **Apache Spark** for batch and streaming ETL
- **Apache Kafka** for real-time ingestion
- **Apache Airflow** for orchestration
- **Great Expectations** for data quality (bonus)

---

## **Project Structure**
```
x_restaurant/
  ├── docs/
  │   ├── architecture.md
  │   ├── bronze_data_model.md
  │   ├── gold_data_model.md
  │   └── silver_data_model.md
  ├── orchestration/
  │   ├── config/
  │   │   ├── airflow_local_settings.py
  │   │   └── airflow.cfg
  │   ├── dags/
  │   │   └── restaurant_pipeline.py
  │   ├── docker-compose.yml
  │   └── logs/
  ├── processing/
  │   ├── demo_data/
  │   │   ├── checkins.csv
  │   │   ├── feedback.csv
  │   │   └── reservations.csv
  │   ├── docker-compose.yml
  │   ├── notebooks/
  │   ├── spark/
  │   │   ├── bronze_to_silver.py
  │   │   ├── create_tables_bronze.py
  │   │   ├── create_tables_gold.py
  │   │   ├── create_tables_silver.py
  │   │   ├── derby.log
  │   │   └── silver_to_gold.py
  │   ├── warehouse/
  │   └── great_expectations/
  ├── streaming/
  │   ├── docker-compose.yml
  │   ├── messages/
  │   │   ├── checkins.json
  │   │   ├── feedback.json
  │   │   └── reservations.json
  │   └── producer.py
  └── README.md
```

---

## **Prerequisites**
- **Docker** & **Docker Compose** ([Install Guide](https://docs.docker.com/get-docker/))
- **Python 3.8+** (for running the Kafka producer locally)
- (Optional) **Git** for cloning the repo

---

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone https://github.com/Momaa73/x_restaurant
git checkout main
cd x_restaurant
```

### **2. Start All Services**
Open **three terminals** (or run sequentially):

**A. Start Streaming (Kafka + Producer):**
```bash
cd streaming
docker-compose build
# Start Kafka
docker-compose up -d
```

**B. Start Processing (Spark + MinIO):**
```bash
cd processing
docker-compose build
# Start Spark and MinIO
docker-compose up -d
```

**C. Start Orchestration (Airflow):**
```bash
cd orchestration
docker-compose build
# Start Airflow
docker-compose up -d
```

### **3. Generate and Send Demo Data**
In the `streaming` folder, depands on your python version (we are using python3):
```bash
python3 producer.py
```
This will send demo reservations, checkins, and feedback to Kafka.

### **4. Trigger the Airflow DAG**
- Open Airflow UI: [http://localhost:8082](http://localhost:8082) (user: airflow, password: airflow)
- Find the `restaurant_pipeline` DAG and **trigger it manually** (or wait for the schedule).

### **5. Monitor Pipeline Progress**
- **Airflow UI:** See task status
- **Spark UI:** [http://localhost:4040](http://localhost:4040) (when jobs are running)
- **MinIO UI:** [http://localhost:9000](http://localhost:9000) (user: admin, password: password)

### **6. Inspect Data**
- **MinIO:** Browse the `warehouse` bucket to see Parquet and metadata files for bronze, silver, and gold tables.
- **Spark SQL:**
  ```bash
  docker exec -it spark-iceberg spark-shell \
  --jars /app/jars/iceberg-spark-runtime.jar \
  --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_catalog.type=hadoop \
  --conf spark.sql.catalog.my_catalog.warehouse=s3a://warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.path.style.access=true
  # In the Spark SQL shell:
  
  # Bronze table query example:
  spark.sql("SELECT * FROM my_catalog.bronze.<bronze_table_name> LIMIT 10").show()

  # Silver table query example:
  spark.sql("SELECT * FROM my_catalog.silver.<silver_table_name> LIMIT 10").show()
  
  # Gold table query example:
  spark.sql("SELECT * FROM my_catalog.gold.<gold_table_name> LIMIT 10").show()
  ```

---

## **Stopping, Backing Up, and Restoring Data**

### **Stop All Services (Safe)**
```bash
docker-compose down
```

### **Back Up Data**
- **MinIO volume:**
  ```bash
  docker run --rm -v minio_data:/data -v $(pwd):/backup busybox cp -r /data /backup/minio_data_backup
  ```
- **Iceberg warehouse:**
  ```bash
  cp -r ./warehouse ./warehouse_backup
  ```

### **Restore Data**
- **MinIO volume:**
  ```bash
  docker run --rm -v minio_data:/data -v $(pwd):/backup busybox cp -r /backup/minio_data_backup/* /data/
  ```
- **Iceberg warehouse:**
  ```bash
  cp -r ./warehouse_backup/* ./warehouse/
  ```

---

## **Data Quality (Bonus: Great Expectations)**
- Minimal [Great Expectations](https://greatexpectations.io/) setup in `processing/great_expectations/`.
- Example suite: `my_suite.expectation_suite.json` (checks `reservation_id` is not null).
- To run a validation:
  ```python
  import great_expectations as ge
  from great_expectations.data_context import FileDataContext
  context = FileDataContext("great_expectations")
  df = ge.read_csv("demo_data/reservations.csv")
  results = context.validate(df, expectation_suite_name="my_suite")
  print(results)
  ```

---

## **Troubleshooting & FAQ**
- **Q: My data is gone after restart!**
  - A: Make sure you didn’t use `docker-compose down -v` (which deletes volumes).
- **Q: Airflow email errors?**
  - A: Email alerting is disabled by default. No SMTP server is needed for local/dev.
- **Q: How do I see my data?**
  - A: Use scala (see above), Spark SQL or browse files in MinIO.
- **Q: How do I reset everything?**
  - A: See backup/restore section above.
- **Q: How do I add more demo data?**
  - A: Edit or add to the JSON files in `streaming/messages/` and rerun the producer.

---

## **Authors / Contributors**
- Moran Benjamin, Eden Adiv, Kori Savadi

---

---

### Note on Null Parameters in Tables

Some columns in the project’s tables—such as the `holiday` field and other semantic or ML-derived parameters—are automatically filled with `null` values. This is intentional: during the mid-semester review, we were required to specify these fields, and for the final project, the pipeline is designed to include them as placeholders. In a real-world scenario, these columns would be populated by integrating with external APIs (e.g., a holiday scheduler) or by running machine learning models to generate the relevant data. For the purposes of this project, their presence demonstrates the intended schema and future extensibility, even though they are not currently populated with real values.

---
