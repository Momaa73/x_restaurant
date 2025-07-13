from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StreamToBronze") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schemas for the topics
reservations_schema = StructType() \
    .add("reservation_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("branch_id", StringType()) \
    .add("reservation_time", TimestampType()) \
    .add("table_type", StringType())

checkins_schema = StructType() \
    .add("checkin_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("branch_id", StringType()) \
    .add("checkin_time", TimestampType())

feedback_schema = StructType() \
    .add("feedback_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("question_id", IntegerType()) \
    .add("score", IntegerType()) \
    .add("submitted_at", TimestampType())

def stream_to_bronze(topic, schema, table):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    query = json_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/checkpoints/{topic}") \
        .option("path", f"my_catalog.bronze.{table}") \
        .trigger(processingTime="10 seconds") \
        .start()

    return query

# Launch all 3 streams
reservations_query = stream_to_bronze("reservations", reservations_schema, "Reservations_raw")
checkins_query = stream_to_bronze("checkins", checkins_schema, "Checkins_raw")
feedback_query = stream_to_bronze("feedback", feedback_schema, "Feedback_raw")

# Await termination
spark.streams.awaitAnyTermination()
