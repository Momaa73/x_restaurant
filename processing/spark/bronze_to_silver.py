from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# -----------------------------
# feedback_cleaned
# -----------------------------
spark.sql("""
INSERT INTO my_catalog.silver.feedback_cleaned
SELECT
    feedback_id,
    phone_number AS customer_id,
    branch_id,
    LENGTH(feedback_text) AS text_length,
    FALSE AS is_holiday,
    'None' AS holiday_type,
    NULL AS shift_manager,
    current_timestamp() AS ingestion_time
FROM my_catalog.bronze.Feedback_raw
""")
print("✅ Inserted into feedback_cleaned")

# -----------------------------
# checkins_cleaned
# -----------------------------
spark.sql("""
INSERT INTO my_catalog.silver.checkins_cleaned
SELECT
    c.checkin_id,
    c.branch_id,
    c.table_id,
    TIMESTAMP(c.checkin_date || ' ' || c.checkin_time) AS checkin_time,
    c.guests_count,
    CASE 
        WHEN r.checkin_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS is_prebooked,
    CASE
        WHEN c.checkin_time < '12:00:00' THEN 'M'
        WHEN c.checkin_time >= '12:00:00' AND c.checkin_time < '17:00:00' THEN 'L'
        ELSE 'E'
    END AS time_of_day_id,
    current_timestamp() AS ingestion_time
FROM my_catalog.bronze.Checkins_raw c
LEFT JOIN my_catalog.silver.reservations_cleaned r
    ON c.checkin_id = r.checkin_id
""")
print("✅ Inserted into checkins_cleaned")

# -----------------------------
# reservations_cleaned (שלב 1 - ללא checkin_id ודיד ארייב)
# -----------------------------
spark.sql("DELETE FROM my_catalog.silver.reservations_cleaned")  # לוודא שאין כפולים אם מריצים שוב

spark.sql("""
INSERT INTO my_catalog.silver.reservations_cleaned
SELECT
    reservation_id,
    phone_number AS customer_id,
    branch_id,
    reservation_date,
    reservation_time,
    guests_count,
    'confirmed' AS status,
    FALSE AS did_arrive,
    NULL AS checkin_id,
    0 AS lead_time_minutes,
    current_timestamp() AS ingestion_time
FROM my_catalog.bronze.Reservations_raw
""")
print("✅ Inserted into reservations_cleaned (initial load)")

# -----------------------------
# reservations_cleaned (שלב 2 - עדכון שדות did_arrive ו-checkin_id)
# -----------------------------
# מייצר טבלה זמנית עם השידוך לפי תאריך + שעה + סניף + טלפון
spark.sql("""
CREATE OR REPLACE TEMP VIEW reservation_checkin_match AS
SELECT
    r.reservation_id,
    c.checkin_id
FROM my_catalog.bronze.Reservations_raw r
JOIN my_catalog.bronze.Checkins_raw c
  ON r.reservation_date = c.checkin_date
 AND r.reservation_time = c.checkin_time
 AND r.branch_id = c.branch_id
 AND r.phone_number = c.customer_phone_number
""")

# מעדכן את השדות המתאימים
spark.sql("""
MERGE INTO my_catalog.silver.reservations_cleaned AS target
USING reservation_checkin_match AS source
ON target.reservation_id = source.reservation_id
WHEN MATCHED THEN
  UPDATE SET
    target.checkin_id = source.checkin_id,
    target.did_arrive = TRUE
""")
print("✅ Updated did_arrive and checkin_id in reservations_cleaned")

# -----------------------------
# customers
# -----------------------------
spark.sql("DELETE FROM my_catalog.silver.customers")  # מרוקן קודם למניעת כפילות

spark.sql("""
INSERT INTO my_catalog.silver.customers
SELECT
    phone_number AS customer_id,
    MAX(customer_name) AS customer_name,
    phone_number,
    COALESCE(checkin_count, 0) AS checkins_count
FROM (
    SELECT customer_name, phone_number FROM my_catalog.bronze.Reservations_raw
    UNION ALL
    SELECT customer_name, phone_number FROM my_catalog.bronze.Feedback_raw
) combined
LEFT JOIN (
    SELECT customer_phone_number, COUNT(*) AS checkin_count
    FROM my_catalog.bronze.Checkins_raw
    GROUP BY customer_phone_number
) counts
ON combined.phone_number = counts.customer_phone_number
GROUP BY phone_number, checkin_count
""")
print("✅ Inserted into customers")
