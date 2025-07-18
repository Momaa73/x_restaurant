from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, length, substring, when, expr
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# --- Helper function to map Spark types to SQL types for Iceberg ---
def spark_type_to_sql_type(data_type):
    if data_type.typeName() == "null":
        return "STRING"
    elif data_type.typeName() == "integer":
        return "INT"
    elif data_type.typeName() == "string":
        return "STRING"
    elif data_type.typeName() == "boolean":
        return "BOOLEAN"
    elif data_type.typeName() == "timestamp":
        return "TIMESTAMP"
    elif data_type.typeName() == "date":
        return "DATE"
    elif data_type.typeName() == "double":
        return "DOUBLE"
    elif data_type.typeName() == "float":
        return "FLOAT"
    elif data_type.typeName() == "long":
        return "BIGINT"
    else:
        return data_type.simpleString().upper()

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# --- טוען את טבלאות הברונז ---
reservations_raw = spark.table("my_catalog.bronze.Reservations_raw")
checkins_raw = spark.table("my_catalog.bronze.Checkins_raw")
feedback_raw = spark.table("my_catalog.bronze.Feedback_raw")

# --- טוען את טבלת הלקוחות הקיימת ---
def table_exists(spark, table_name):
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False

# Ensure the silver schema exists in the catalog before creating tables
spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.silver")

# Ensure the customers table exists using Spark SQL DDL
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver.customers (
    customer_id INT,
    customer_name STRING,
    phone_number STRING
)
USING ICEBERG
""")

customers_table = "my_catalog.silver.customers"
if table_exists(spark, customers_table):
    customers_df = spark.table(customers_table)
else:
    # Create empty DataFrame with the correct schema
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("phone_number", StringType(), True)
    ])
    customers_df = spark.createDataFrame([], customers_schema)
    # No need to write to create the table, DDL already did it

# --- איחוד לקוחות ייחודיים מכל טבלאות הברונז ---
reservations_customers = reservations_raw.select("customer_name", "phone_number").distinct()
checkins_customers = checkins_raw.select("customer_name", "phone_number").distinct()
feedback_customers = feedback_raw.select("customer_name", "phone_number").distinct()

all_new_customers = reservations_customers.union(checkins_customers).union(feedback_customers).distinct()

# --- מוצא את הלקוחות שכבר קיימים לפי מספר טלפון ---
existing_phones = customers_df.select("phone_number").distinct()

# --- מסנן לקוחות חדשים שלא קיימים בטבלה ---
new_customers = all_new_customers.join(existing_phones, "phone_number", "left_anti")

# --- אם יש לקוחות חדשים, מוסיף אותם עם מזהה עולה ---
if new_customers.count() > 0:
    max_id = customers_df.select(spark_max(col("customer_id"))).collect()[0][0]
    next_id = 1 if max_id is None else max_id + 1

    window = Window.orderBy("phone_number")
    new_customers_with_id = new_customers.withColumn("customer_id", row_number().over(window) + next_id - 1)
    new_customers_with_id = new_customers_with_id.select("customer_id", "customer_name", "phone_number")

    new_customers_with_id.write.format("iceberg").mode("append").save("my_catalog.silver.customers")
    print(f"הוספו {new_customers_with_id.count()} לקוחות חדשים לטבלת customers.")
else:
    print("אין לקוחות חדשים להוסיף לטבלת customers.")

# --- Process reservations_cleaned ---
reservations_cleaned = reservations_raw.select(
    "reservation_id",
    "customer_name",
    "phone_number",
    "branch_id",
    "table_id",
    "reservation_date",
    "reservation_hour",
    "guests_count",
    col("created_at").cast("timestamp").alias("created_at"),
    "limited_hours",
    "hours_if_limited"
).withColumn("created_at_date", col("created_at").cast("date")) \
 .withColumn("created_at_hour", substring(col("created_at").cast("string"), 12, 5)) \
 .drop("created_at") \
 .withColumn("is_holiday", when(col("reservation_id").isNotNull(), False).otherwise(False).cast("boolean")) \
 .withColumn("holiday_name", when(col("reservation_id").isNotNull(), None).otherwise(None).cast("string")) \
 .withColumn("ingestion_time", current_timestamp())

cols_to_check = [c for c in reservations_cleaned.columns if c != "ingestion_time"]
reservations_cleaned = reservations_cleaned.dropDuplicates(cols_to_check)

# --- Ensure reservations_cleaned table exists ---
reservations_schema = ", ".join([
    f"{field.name} {spark_type_to_sql_type(field.dataType)}" for field in reservations_cleaned.schema.fields
])
create_reservations_table_sql = f"""
CREATE TABLE IF NOT EXISTS my_catalog.silver.reservations_cleaned (
    {reservations_schema}
)
USING ICEBERG
"""
spark.sql(create_reservations_table_sql)

reservations_cleaned.write.mode("append").format("iceberg").save("my_catalog.silver.reservations_cleaned")

# --- Helper function to get time_of_day_id based on time string ---
def get_time_of_day_id(time_col):
    return when((time_col >= "08:00") & (time_col < "12:00"), "M") \
           .when((time_col >= "12:00") & (time_col < "17:00"), "L") \
           .when((time_col >= "17:00") & (time_col < "23:00"), "E") \
           .otherwise(None)

# --- Process checkins_cleaned ---
checkins_cleaned = checkins_raw.select(
    "checkin_id",
    "customer_name",
    "phone_number",
    "branch_id",
    "table_id",
    "is_prebooked",
    "checkin_date",
    "checkin_time",
    "guests_count",
    "shift_manager"
).withColumn("time_of_day_id", get_time_of_day_id(col("checkin_time"))) \
 .withColumn("is_holiday", when(col("checkin_id").isNotNull(), False).otherwise(False).cast("boolean")) \
 .withColumn("holiday_name", when(col("checkin_id").isNotNull(), None).otherwise(None).cast("string")) \
 .withColumn("ingestion_time", current_timestamp()) \
 .drop("checkin_time")

cols_to_check = [c for c in checkins_cleaned.columns if c != "ingestion_time"]
checkins_cleaned = checkins_cleaned.dropDuplicates(cols_to_check)

# --- Ensure checkins_cleaned table exists ---
checkins_schema = ", ".join([
    f"{field.name} {spark_type_to_sql_type(field.dataType)}" for field in checkins_cleaned.schema.fields
])
create_checkins_table_sql = f"""
CREATE TABLE IF NOT EXISTS my_catalog.silver.checkins_cleaned (
    {checkins_schema}
)
USING ICEBERG
"""
spark.sql(create_checkins_table_sql)

checkins_cleaned.write.mode("append").format("iceberg").save("my_catalog.silver.checkins_cleaned")

# --- Process feedback_cleaned ---
feedback_cleaned = feedback_raw.select(
    "feedback_id",
    "branch_id",
    "customer_name",
    "phone_number",
    "feedback_text",
    "rating",
    "dining_date",
    "dining_time"
).withColumn("text_length", length(col("feedback_text"))) \
 .withColumn("dining_time_of_day_id", get_time_of_day_id(col("dining_time"))) \
 .withColumn("is_holiday", when(col("feedback_id").isNotNull(), False).otherwise(False).cast("boolean")) \
 .withColumn("holiday_name", when(col("feedback_id").isNotNull(), None).otherwise(None).cast("string")) \
 .withColumn("ingestion_time", current_timestamp()) \
 .drop("dining_time")

cols_to_check = [c for c in feedback_cleaned.columns if c != "ingestion_time"]
feedback_cleaned = feedback_cleaned.dropDuplicates(cols_to_check)

# --- Ensure feedback_cleaned table exists ---
feedback_schema = ", ".join([
    f"{field.name} {spark_type_to_sql_type(field.dataType)}" for field in feedback_cleaned.schema.fields
])
create_feedback_table_sql = f"""
CREATE TABLE IF NOT EXISTS my_catalog.silver.feedback_cleaned (
    {feedback_schema}
)
USING ICEBERG
"""
spark.sql(create_feedback_table_sql)

feedback_cleaned.write.mode("append").format("iceberg").save("my_catalog.silver.feedback_cleaned")

# --- Ensure reservations_cleaned table exists ---
# Check for NullType columns and raise error if found
nulltype_cols = [field.name for field in reservations_cleaned.schema.fields if field.dataType.typeName() == "null"]
if nulltype_cols:
    print("ERROR: NullType columns found in reservations_cleaned schema:", nulltype_cols)
    print(reservations_cleaned.schema)
    raise Exception(f"NullType columns found in reservations_cleaned: {nulltype_cols}")

# --- Ensure checkins_cleaned table exists ---
nulltype_cols = [field.name for field in checkins_cleaned.schema.fields if field.dataType.typeName() == "null"]
if nulltype_cols:
    print("ERROR: NullType columns found in checkins_cleaned schema:", nulltype_cols)
    print(checkins_cleaned.schema)
    raise Exception(f"NullType columns found in checkins_cleaned: {nulltype_cols}")

# --- Ensure feedback_cleaned table exists ---
nulltype_cols = [field.name for field in feedback_cleaned.schema.fields if field.dataType.typeName() == "null"]
if nulltype_cols:
    print("ERROR: NullType columns found in feedback_cleaned schema:", nulltype_cols)
    print(feedback_cleaned.schema)
    raise Exception(f"NullType columns found in feedback_cleaned: {nulltype_cols}")

# --- Process reservations_cleaned (last 48 hours) ---
reservations_recent = reservations_cleaned.filter(
    expr("created_at_date >= date_sub(current_date(), 2)")
)
reservations_recent.createOrReplaceTempView("reservations_updates")

# Upsert into silver.reservations_cleaned using MERGE INTO
spark.sql("""
MERGE INTO my_catalog.silver.reservations_cleaned t
USING reservations_updates s
ON t.reservation_id = s.reservation_id AND t.created_at_date = s.created_at_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# --- Process checkins_cleaned (last 48 hours) ---
checkins_recent = checkins_cleaned.filter(
    expr("checkin_date >= date_sub(current_date(), 2)")
)
checkins_recent.createOrReplaceTempView("checkins_updates")

spark.sql("""
MERGE INTO my_catalog.silver.checkins_cleaned t
USING checkins_updates s
ON t.checkin_id = s.checkin_id AND t.checkin_date = s.checkin_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# --- Process feedback_cleaned (last 48 hours) ---
feedback_recent = feedback_cleaned.filter(
    expr("dining_date >= date_sub(current_date(), 2)")
)
feedback_recent.createOrReplaceTempView("feedback_updates")

spark.sql("""
MERGE INTO my_catalog.silver.feedback_cleaned t
USING feedback_updates s
ON t.feedback_id = s.feedback_id AND t.dining_date = s.dining_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.stop()