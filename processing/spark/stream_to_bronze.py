from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Kafka topic names
TOPICS = {
    "reservations": "reservations_topic",
    "checkins": "checkins_topic",
    "feedback": "feedback_topic"
}

# Spark session
spark = SparkSession.builder \
    .appName("StreamToBronze") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Define schemas
reservation_schema = StructType([
    StructField("reservation_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("phone_number", StringType()),
    StructField("branch_id", IntegerType()),
    StructField("reservation_time", TimestampType()),
    StructField("guests_count", IntegerType()),
    StructField("created_at", TimestampType()),
    StructField("limited_hours", BooleanType()),
    StructField("hours_if_limited", FloatType()),
    StructField("table_type", StringType()),
    StructField("table_location", StringType())
])

checkin_schema = StructType([
    StructField("checkin_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("phone_number", StringType()),
    StructField("branch_id", IntegerType()),
    StructField("table_id", IntegerType()),
    StructField("is_prebooked", BooleanType()),
    StructField("checkin_time", TimestampType()),
    StructField("guests_count", IntegerType()),
    StructField("shift_manager", StringType())
])

feedback_schema = StructType([
    StructField("feedback_id", IntegerType()),
    StructField("branch_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("phone_number", StringType()),
    StructField("customer_name", StringType()),
    StructField("feedback_text", StringType()),
    StructField("rating", IntegerType()),
    StructField("dining_date", DateType()),
    StructField("dining_time", StringType()),
    StructField("submission_time", TimestampType())
])

# Read & write function
def stream_topic(topic, schema, table_name):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query = parsed.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/checkpoints/{table_name}") \
        .option("path", f"my_catalog.bronze.{table_name}") \
        .start()

    return query

# Start streaming
q1 = stream_topic(TOPICS["reservations"], reservation_schema, "Reservations_raw")
q2 = stream_topic(TOPICS["checkins"], checkin_schema, "Checkins_raw")
q3 = stream_topic(TOPICS["feedback"], feedback_schema, "Feedback_raw")

# Wait for termination
spark.streams.awaitAnyTermination()
