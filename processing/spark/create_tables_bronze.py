from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CreateIcebergTables") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.my_catalog.default-namespace", "bronze") \
    .getOrCreate()

# spark.sql("DROP TABLE IF EXISTS my_catalog.Reservations_raw")
# spark.sql("DROP TABLE IF EXISTS my_catalog.Checkins_raw")
# spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.Feedback_raw")

# dummy comment for commit first time
# dummy comment for commit second time

spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.bronze")

# Reservations_raw
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.bronze.Reservations_raw (
    reservation_id INT,
    customer_name STRING,
    phone_number STRING,
    branch_id INT,
    table_id INT,
    reservation_date DATE, -- when will the customers arrive ?
    reservation_hour STRING,
    guests_count INT,
    created_at TIMESTAMP,
    limited_hours BOOLEAN,
    hours_if_limited FLOAT
) USING iceberg
""")

# Checkins_raw
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.bronze.Checkins_raw (
    checkin_id INT,
    customer_name STRING,
    phone_number STRING,
    branch_id INT,
    table_id INT,
    is_prebooked BOOLEAN,
    checkin_date DATE,
    checkin_time STRING,
    guests_count INT,
    shift_manager STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.bronze.Feedback_raw (
    feedback_id INT, 
    branch_id INT,             
    customer_name STRING, 
    phone_number STRING,   
    feedback_text STRING,
    rating INT,
    dining_date DATE,
    dining_time STRING, 
    submission_time TIMESTAMP    
) USING iceberg
""")

spark.stop()
