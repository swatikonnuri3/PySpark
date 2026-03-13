import os
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder \
    .appName("SCD_Type1") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── STEP 1: Existing data (already in your warehouse) ──
existing_data = [
    (1, "Swati",  "Mumbai"),
    (2, "Rahul",  "Delhi"),
    (3, "Anjali", "Chennai")
]
existing_df = spark.createDataFrame(existing_data, ["id", "name", "city"])

print("STEP 1 - EXISTING DATA (Before Update):")
existing_df.show()

# ── STEP 2: Incoming new data (from source system today) ──
new_data = [
    (1, "Swati",  "Pune"),       # city changed
    (2, "Rahul",  "Delhi"),      # no change
    (4, "Vikram", "Bangalore")   # new record
]
new_df = spark.createDataFrame(new_data, ["id", "name", "city"])

print("STEP 2 - INCOMING NEW DATA:")
new_df.show()

# ── STEP 3: Full outer join to combine both ──
joined_df = existing_df.alias("old").join(
    new_df.alias("new"),
    on="id",
    how="full_outer"
)

print("STEP 3 - AFTER FULL OUTER JOIN (internal view):")
joined_df.show()

# ── STEP 4: Apply SCD Type 1 logic (if new exists → take new, else keep old) ──
scd1_df = joined_df.select(
    when(col("new.id").isNotNull(),   col("new.id"))  .otherwise(col("old.id"))  .alias("id"),
    when(col("new.name").isNotNull(), col("new.name")).otherwise(col("old.name")).alias("name"),
    when(col("new.city").isNotNull(), col("new.city")).otherwise(col("old.city")).alias("city")
)

print("STEP 4 - FINAL RESULT AFTER SCD TYPE 1:")
scd1_df.show()

# ── STEP 5: Save to CSV ──
scd1_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("C:/Users/Swati/PycharmProjects/PySpark/data/scd_type1")

print("SCD Type 1 Done! File saved.")
spark.stop()