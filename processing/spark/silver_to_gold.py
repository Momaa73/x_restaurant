from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_date, current_timestamp, lit,
    col, when, row_number, to_timestamp, concat_ws,
    unix_timestamp, date_format, first
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SilverToGold") \
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

# --- טוענים טבלאות Silver ---
customers = spark.table("my_catalog.silver.customers")
scd2_branch = spark.table("my_catalog.silver.scd2_branch")
tables = spark.table("my_catalog.silver.table")
reservations = spark.table("my_catalog.silver.reservations_cleaned")
checkins = spark.table("my_catalog.silver.checkins_cleaned")
feedback = spark.table("my_catalog.silver.feedback_cleaned")

# --- תאריך ושעה נוכחית ---
today = current_date()
current_ts = current_timestamp()
current_time_str = date_format(current_ts, "HH:mm")

# --- פונקציה למיפוי שעות ל-M/L/E (time_of_day_id) ---
def map_hour_to_time_of_day(hour_col):
    return when((hour_col >= "08:00") & (hour_col < "12:00"), "M") \
        .when((hour_col >= "12:00") & (hour_col < "17:00"), "L") \
        .when((hour_col >= "17:00") & (hour_col < "23:00"), "E") \
        .otherwise(None)

# --------- 1. fact_reservations -----------

# מצרפים customer_id מ-customers לפי phone_number
reservations_with_customer = reservations.join(
    customers.select("customer_id", "phone_number"),
    on="phone_number",
    how="left"
)

# מצרפים מידע על סניפים
reservations_with_branch = reservations_with_customer.join(
    scd2_branch.select("branch_id", "city"),
    on="branch_id",
    how="left"
)

# מצרפים מידע על שולחנות
reservations_with_table = reservations_with_branch.join(
    tables.select("table_id", "location_type_id", "table_type", "seat_count"),
    on="table_id",
    how="left"
)

# מחשבים עמודת datetime של ההזמנה (תאריך + שעה)
reservations_with_table = reservations_with_table.withColumn(
    "reservation_datetime",
    to_timestamp(concat_ws(" ", col("reservation_date").cast("string"), col("reservation_hour")))
)

# מחשבים סטטוס ההזמנה - האם המועד כבר עבר
reservations_with_table = reservations_with_table.withColumn(
    "status",
    when(current_ts >= col("reservation_datetime"), lit("Relevant")).otherwise(lit("NotYet"))
)

# ממפים את reservation_hour ל-time_of_day_id לצורך join עם checkins.time_of_day_id
reservations_with_table = reservations_with_table.withColumn(
    "reservation_time_of_day_id",
    map_hour_to_time_of_day(col("reservation_hour"))
)

# מכינים checkins למיזוג: בוחרים את העמודות הרלוונטיות ומקבלים alias
checkins_for_join = checkins.select(
    col("checkin_id"),
    col("customer_name").alias("chk_customer_name"),
    col("phone_number").alias("chk_phone_number"),
    col("checkin_date"),
    col("time_of_day_id").alias("chk_time_of_day_id")
)

# מבצעים join בין reservations ל-checkins לפי שם, טלפון, תאריך, וזמן ביום
reservations_with_checkin = reservations_with_table.join(
    checkins_for_join,
    (reservations_with_table.customer_name == checkins_for_join.chk_customer_name) &
    (reservations_with_table.phone_number == checkins_for_join.chk_phone_number) &
    (reservations_with_table.reservation_date == checkins_for_join.checkin_date) &
    (reservations_with_table.reservation_time_of_day_id == checkins_for_join.chk_time_of_day_id),
    how="left"
)

# מחשבים arrival_status לפי הסטטוס והאם קיימת כניסה (checkin_id)
reservations_with_checkin = reservations_with_checkin.withColumn(
    "arrival_status",
    when(col("status") == "NotYet", lit("NotYet"))
    .when(col("checkin_id").isNotNull(), lit("Arrived"))
    .otherwise(lit("NotArrived"))
)

# מגדירים את checkin_id לפי מצב הסטטוס
reservations_with_checkin = reservations_with_checkin.withColumn(
    "checkin_id",
    when(col("status") == "NotYet", lit("0"))
    .otherwise(col("checkin_id"))
)

# חישוב lead_time_minutes: הפרש בדקות בין ingestion_time לבין מועד ההזמנה
reservations_with_checkin = reservations_with_checkin.withColumn(
    "lead_time_minutes",
    ((unix_timestamp(col("reservation_datetime")) - unix_timestamp(col("ingestion_time"))) / 60).cast("int")
)

# חישוב is_update לפי window – השורה הכי עדכנית לפי phone_number ו-reservation_date
window_update = Window.partitionBy("phone_number", "reservation_date").orderBy(col("ingestion_time").desc())

reservations_with_checkin = reservations_with_checkin.withColumn(
    "row_num",
    row_number().over(window_update)
).withColumn(
    "is_update",
    when(col("row_num") == 1, lit(True)).otherwise(lit(False))
).drop("row_num")

# בחירת העמודות הסופיות לטבלת fact_reservations
fact_reservations_df = reservations_with_checkin.select(
    "reservation_id",
    "customer_id",
    "customer_name",
    "phone_number",
    "branch_id",
    "city",
    "table_id",
    "location_type_id",
    "table_type",
    "seat_count",
    "reservation_date",
    "reservation_hour",
    "guests_count",
    "created_at_date",
    "created_at_hour",
    "status",
    "limited_hours",
    "hours_if_limited",
    "is_holiday",
    "holiday_name",
    "arrival_status",
    "checkin_id",
    "lead_time_minutes",
    "is_update",
    "ingestion_time"
)

fact_reservations_df.write.format("iceberg").mode("append").save("my_catalog.gold.fact_reservations")


# --------- 2. fact_daily_per_branch -----------

from pyspark.sql.functions import sum as spark_sum

# סינון להזמנות ו-checkins של היום
reservations_today = reservations.filter(col("reservation_date") == today)
checkins_today = checkins.filter(col("checkin_date") == today)

# מידע על סניפים כולל פתיחה/סגירה
branch_info = scd2_branch.select(
    "branch_id", "branch_name", "city", "capacity",
    (col("closing_date").isNull()).alias("is_branch_open")
)

# חישובים שונים:
total_reservations_df = reservations_today.groupBy("branch_id").count().withColumnRenamed("count", "total_reservations")
total_checkins_df = checkins_today.groupBy("branch_id").count().withColumnRenamed("count", "total_checkins")
checkins_prebooked_df = checkins_today.filter(col("is_prebooked") == True).groupBy("branch_id").count().withColumnRenamed("count", "checkins_from_reservations")

# כניסות ללא הזמנה מראש
real_time_checkin_df = total_checkins_df.join(
    checkins_prebooked_df,
    on="branch_id",
    how="left"
).withColumn(
    "real_time_checkint",
    col("total_checkins") - col("checkins_from_reservations")
).select("branch_id", "real_time_checkint")

# כניסות לפי זמן ביום (M/L/E)
dining_time_counts = checkins_today.groupBy("branch_id", "time_of_day_id").agg(
    spark_sum("guests_count").alias("count")
).groupBy("branch_id").pivot("time_of_day_id", ["M", "L", "E"]).sum("count").na.fill(0)

# סך כל האורחים היום
total_guests_df = checkins_today.groupBy("branch_id").agg(
    spark_sum("guests_count").alias("total_guests")
)

# הוספת עמודות is_holiday, holiday_name, shift_manager מה-checkins (קח את הערכים הראשונים לפי כל סניף)
holiday_shift_df = checkins_today.groupBy("branch_id").agg(
    first("is_holiday", ignorenulls=True).alias("is_holiday"),
    first("holiday_name", ignorenulls=True).alias("holiday_name"),
    first("shift_manager", ignorenulls=True).alias("shift_manager")
)

# ... הקוד שלך לפני זה ...

# חישוב כניסות לפי זמן ביום
dining_time_counts = checkins_today.groupBy("branch_id", "time_of_day_id").agg(
    spark_sum("guests_count").alias("count")
).groupBy("branch_id").pivot("time_of_day_id", ["M", "L", "E"]).sum("count").na.fill(0)

# ודא שכל העמודות קיימות, אם לא - צור עם ערך 0
for col_name in ["M", "L", "E"]:
    if col_name not in dining_time_counts.columns:
        dining_time_counts = dining_time_counts.withColumn(col_name, lit(0))

# שנה שמות עמודות לפי הדרישה
dining_time_counts = dining_time_counts.withColumnRenamed("M", "dining_M") \
                                       .withColumnRenamed("L", "dining_L") \
                                       .withColumnRenamed("E", "dining_E")

# סך כל האורחים היום
total_guests_df = checkins_today.groupBy("branch_id").agg(
    spark_sum("guests_count").alias("total_guests")
)
# בניית טבלת סיכום יומית לפי סניף
fact_daily_df = branch_info.join(total_reservations_df, "branch_id", "left") \
    .join(total_checkins_df, "branch_id", "left") \
    .join(checkins_prebooked_df, "branch_id", "left") \
    .join(real_time_checkin_df, "branch_id", "left") \
    .join(dining_time_counts, "branch_id", "left") \
    .join(total_guests_df, "branch_id", "left") \
    .join(holiday_shift_df, "branch_id", "left") \
    .withColumn("day", today) \
    .withColumn("avg_occupancy_rate", col("total_guests") / col("capacity")) \
    .withColumnRenamed("branch_name", "branch_name") \
    .withColumnRenamed("city", "city") \
    .withColumnRenamed("capacity", "capacity") \
    .withColumnRenamed("is_branch_open", "is_branch_open") \
    .withColumn("ingestion_time", current_timestamp())

fact_daily_df.write.format("iceberg").mode("append").save("my_catalog.gold.fact_daily_per_branch")


# --------- 3. feedback_per_branch -----------

# תחילת וסוף שבוע סטטיים (ניתן לשנות לתאריכים דינמיים לפי צורך)
week_start = lit("2025-07-14").cast("date")
week_end = lit("2025-07-20").cast("date")

feedback_branch = feedback.join(
    scd2_branch.select("branch_id", "branch_name", "city", "closing_date"),
    "branch_id",
    "left"
).withColumn(
    "is_branch_open",
    when(col("closing_date").isNull(), True).otherwise(False)
).withColumn("week_start", week_start).withColumn("week_end", week_end).withColumn(
    "shift_managers", lit(None).cast("string")
).withColumn(
    "semantic_label", lit(None).cast("string")
).withColumn(
    "semantic_category", lit(None).cast("string")
).withColumn(
    "avg_rating", lit(None).cast("float")
).withColumn(
    "ingestion_time", current_timestamp()
)

feedback_branch = feedback_branch.select(
    "feedback_id",
    "branch_id",
    "branch_name",
    "city",
    "is_branch_open",
    "week_start",
    "week_end",
    "customer_name",
    "phone_number",
    "feedback_text",
    "rating",
    "text_length",
    "dining_date",
    "dining_time_of_day_id",
    "is_holiday",
    "holiday_name",
    "shift_managers",
    "semantic_label",
    "semantic_category",
    "avg_rating",
    "ingestion_time"
)

feedback_branch.write.format("iceberg").mode("append").save("my_catalog.gold.feedback_per_branch")


# --------- 4. customers (gold) -----------

feedback_counts = feedback.groupBy("phone_number").count().withColumnRenamed("count", "feedback_count")

customers_gold = customers.join(feedback_counts, "phone_number", "left").withColumn(
    "feedback_count",
    when(col("feedback_count").isNull(), lit(0)).otherwise(col("feedback_count"))
).withColumn("ingestion_time", current_timestamp())

customers_gold = customers_gold.select(
    "customer_id",
    "customer_name",
    "phone_number",
    "feedback_count",
    "ingestion_time"
)

customers_gold.write.format("iceberg").mode("append").save("my_catalog.gold.customers")


spark.stop()
