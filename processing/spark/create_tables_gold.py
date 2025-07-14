from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CreateGoldTables") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalog.my_catalog.default-namespace", "gold") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.gold")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.gold.fact_reservations (
    reservation_id	INT,
    customer_id	INT,
    customer_name STRING,
    phone_number STRING,
    branch_id INT,
    city STRING,
    table_id INT,
    location_type_id INT,    -- e.g., Indoor / Outdoor / Covered
    table_type STRING,          -- e.g., Round / Bar / Regular
    seat_count INT,             -- Number of seats
    reservation_date DATE,
    reservation_hour STRING,
    guests_count INT,
    created_at_date DATE,
    created_at_hour STRING,
    status STRING,
    limited_hours BOOLEAN,
    hours_if_limited FLOAT,
    is_holiday BOOLEAN,
    holiday_name STRING,
    arrival_status STRING,
    checkin_id STRING, -- 0 If the deadline has not arrived , null if did not arrive
    lead_time_minutes INT,
    is_update BOOLEAN, -- in case someone changed table or number of guests and there is a new updated raw
    ingestion_time TIMESTAMP
) USING iceberg;
""")


spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.gold.fact_daily_per_branch (
    raw_id INT,
    branch_id INT,
    branch_name STRING,
    city STRING,
    capacity INT,
    is_branch_open BOOLEAN,
    day DATE,
    total_reservations INT,
    total_checkins INT,
    checkins_from_reservations INT,
    real_time_checkint INT,
    dining_M INT, 
    dining_L INT, 
    dining_E INT, 
    total_guests INT,
    avg_occupancy_rate FLOAT,
    is_holiday BOOLEAN,
    holiday_name STRING,
    shift_manager STRING,
    ingestion_time TIMESTAMP 
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.gold.feedback_per_branch (
    feedback_id INT,
    branch_id INT,
    branch_name STRING,
    city STRING,
    is_branch_open BOOLEAN,
    week_start DATE,
    week_end DATE,
    customer_name STRING,
    phone_number STRING,   
    feedback_text STRING,
    rating INT,
    text_length INT,
    dining_date DATE,
    dining_time_of_day_id STRING, 
    is_holiday BOOLEAN,
    holiday_name STRING,
    shift_managers STRING,
    semantic_label STRING,      -- FILLED IN ONLY AFTER THE ML MODEL
    semantic_category STRING,   -- FILLED IN ONLY AFTER THE ML MODEL
    avg_rating FLOAT,
    ingestion_time TIMESTAMP
) USING iceberg;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.customers (
    customer_id INT,  -- phone_number will serve as the ID
    customer_name STRING,
    phone_number STRING,
    feedback_count INT,
    ingestion_time TIMESTAMP
) USING iceberg;
""")