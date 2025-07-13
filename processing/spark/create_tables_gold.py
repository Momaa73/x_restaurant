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
CREATE TABLE IF NOT EXISTS my_catalog.gold.Reservations_raw (
    reservation_id	INT, ------------------------------------------------ עצרתי כאן, להמשיך !!!
    customer_id	INT,
    branch_id	INT
    table_id	INT
    reservation_date	DATE
    reservation_time	TIME
    guests_count	INT
    status	STRING
    did_arrive	BOOLEAN
    checkin_id	STRING
    lead_time_minutes	INT
    feedback_id	STRING
    ingestion_time	DATETIME
""")

          
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.gold.Reservations_raw (
Column name	Data type
branch_id	STRING
date	DATE
total_reservations	INT
total_checkins	INT
total_guests	INT
avg_occupancy_rate	FLOAT
avg_lead_time	FLOAT
avg_sentiment_score	FLOAT
ingestion_time	DATETIME
""")
          