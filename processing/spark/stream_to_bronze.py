import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, TimestampType, FloatType

spark = SparkSession.builder \
    .appName("StreamToBronze") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

reservation_schema = StructType([
    StructField("reservation_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("phone_number", StringType()),
    StructField("branch_id", IntegerType()),
    StructField("table_id", IntegerType()),
    StructField("reservation_date", DateType()),
    StructField("reservation_hour", StringType()),
    StructField("guests_count", IntegerType()),
    StructField("created_at", TimestampType()),
    StructField("limited_hours", BooleanType()),
    StructField("hours_if_limited", FloatType())
])

checkin_schema = StructType([
    StructField("checkin_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("phone_number", StringType()),
    StructField("branch_id", IntegerType()),
    StructField("table_id", IntegerType()),
    StructField("is_prebooked", BooleanType()),
    StructField("checkin_date", DateType()),
    StructField("checkin_time", StringType()),
    StructField("guests_count", IntegerType()),
    StructField("shift_manager", StringType())
])

feedback_schema = StructType([
    StructField("feedback_id", IntegerType()),
    StructField("branch_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("phone_number", StringType()),
    StructField("feedback_text", StringType()),
    StructField("rating", IntegerType()),
    StructField("dining_date", DateType()),
    StructField("dining_time", StringType()),
    StructField("submission_time", TimestampType())
])

def read_kafka(topic, schema, checkpoint_path, output_path):
    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df_raw.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # Add watermarking and deduplication for late-arriving data (up to 48 hours)
    if topic == "reservations":
        # Use created_at as event time
        df = df.withWatermark("created_at", "48 hours")
        df = df.dropDuplicates(["reservation_id", "created_at"])
    elif topic == "checkins":
        # Use checkin_time as event time (cast to timestamp if needed)
        # If checkin_time is string, you may want to cast to timestamp for watermarking
        # For simplicity, we'll use checkin_date and checkin_time as a composite key
        df = df.withColumn("checkin_timestamp", col("checkin_date").cast("string") + " " + col("checkin_time"))
        df = df.withWatermark("checkin_timestamp", "48 hours")
        df = df.dropDuplicates(["checkin_id", "checkin_timestamp"])
    elif topic == "feedback":
        # Use submission_time as event time
        df = df.withWatermark("submission_time", "48 hours")
        df = df.dropDuplicates(["feedback_id", "submission_time"])

    df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", output_path) \
        .start()

run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "default_run")

read_kafka(
    topic="reservations",
    schema=reservation_schema,
    checkpoint_path=f"/tmp/checkpoints/reservations/{run_id}",
    output_path="my_catalog.bronze.Reservations_raw"
)

read_kafka(
    topic="checkins",
    schema=checkin_schema,
    checkpoint_path=f"/tmp/checkpoints/checkins/{run_id}",
    output_path="my_catalog.bronze.Checkins_raw"
)

read_kafka(
    topic="feedback",
    schema=feedback_schema,
    checkpoint_path=f"/tmp/checkpoints/feedback/{run_id}",
    output_path="my_catalog.bronze.Feedback_raw"
)

spark.streams.awaitAnyTermination(timeout=60)
