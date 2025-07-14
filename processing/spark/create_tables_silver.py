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
    branch_id INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE my_catalog.silver.scd2_shift_schedule (
    manager_id INT,
    branch_id INT,
    time_id STRING,         -- M / L / E
    is_current BOOLEAN
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.scd2_branch (
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
CREATE TABLE IF NOT EXISTS my_catalog.silver.dim_table (
    table_id INT,            -- PRIMARY KEY
    branch_id INT,           -- FK → dim_branch.branch_id
    location_type_id INT,    -- e.g., Indoor / Outdoor / Covered
    table_type STRING,          -- e.g., Round / Bar / Regular -----------------------------------------fill by table_id
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
    branch_id INT,
    customer_name STRING,
    phone_number STRING,   
    feedback_text STRING,
    rating INT,
    text_length INT,
    dining_date DATE,
    dining_time_of_day STRING, 
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
    customer_name STRING,
    phone_number STRING,
    branch_id INT,
    table_id INT,
    is_prebooked BOOLEAN,
    checkin_date DATE,
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
    hours_if_limited FLOAT,
    is_holiday BOOLEAN,
    holiday_name STRING,
    ingestion_time TIMESTAMP
) USING iceberg;
""")