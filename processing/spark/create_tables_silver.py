from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CreateSilverTables") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.my_catalog.default-namespace", "silver") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.silver")


spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.customers (
    customer_id INT,  -- phone_number will serve as the ID
    customer_name STRING,
    phone_number STRING,
    feedback_count INT
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.dim_shift_managers (
    shift_manager_id INT,
    name STRING,
    phone_number STRING,
    branch_id INT,
) USING iceberg
""")

spark.sql("""
CREATE TABLE my_catalog.silver.dim_shift_schedule (
    manager_id STRING,
    branch_id STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.dim_branch (
    branch_id INT,
    branch_name STRING,
    city STRING,
    address STRING,
    capacity INT,
    opening_date DATE,
    closing_date DATE,
    is_update BOOLEAN
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.table_details (
    table_id INT,            -- PRIMARY KEY
    branch_id INT,           -- FK → dim_branch.branch_id
    location_type_id INT,    -- e.g., Indoor / Outdoor / Covered
    table_type STRING,          -- e.g., Round / Bar / Regular
    table_number STRING,        -- Optional logical number
    seat_count INT,             -- Number of seats
    is_update BOOLEAN        
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.dim_time_of_day (
    time_id STRING,         -- M / L / E
    time_label STRING       -- Morning / Lunch / Evening
) USING iceberg;
""")

# feedback_cleaned
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.feedback_cleaned (
    feedback_id INT,
    customer_id INT,
    branch_id INT,
    feedback_text STRING,
    text_length INT,
    is_holiday BOOLEAN,
    holiday_name STRING,
    shift_manager STRING,
    ingestion_time TIMESTAMP
) USING iceberg;
""")

# checkins_cleaned
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.checkins_cleaned (
    checkin_id INT,
    customer_id INT,
    phone_number STRING,
    branch_id INT,
    table_id INT,
    is_prebooked BOOLEAN,
    time_of_day_id STRING,
    guests_count INT,
    shift_manager STRING,
    is_holiday BOOLEAN,
    holiday_name STRING,
    ingestion_time TIMESTAMP
) USING iceberg;
""")

# reservations_cleaned
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.reservations_cleaned (
    reservation_id STRING,
    customer_id STRING,
    branch_id STRING,
    reservation_time TIMESTAMP,
    guests_count INT,
    status STRING,
    arrival_status STRING,
    checkin_id STRING,
    lead_time_minutes INT,
    is_holiday BOOLEAN,
    holiday_name STRING,
    is_updated BOOLEAN,
    ingestion_time TIMESTAMP
) USING iceberg;
""")