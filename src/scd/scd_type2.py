import os
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

spark = SparkSession.builder.appName("SCD_Type2").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Existing Data ──
existing_schema = StructType([
    StructField("id",         IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("city",       StringType(),  True),
    StructField("is_active",  BooleanType(), True),
    StructField("start_date", StringType(),  True),
    StructField("end_date",   StringType(),  True)
])

existing_df = spark.createDataFrame([
    (1, "Swati",  "Mumbai",  True,  "2023-01-01", None),
    (2, "Rahul",  "Delhi",   True,  "2023-01-01", None),
    (3, "Anjali", "Chennai", True,  "2023-01-01", None)
], existing_schema)

print("EXISTING DATA:")
existing_df.show()

# ── New Incoming Data ──
new_df = spark.createDataFrame([
    (1, "Swati",  "Pune"),
    (2, "Rahul",  "Delhi"),
    (4, "Vikram", "Bangalore")
], ["id", "name", "city"])

print("NEW DATA:")
new_df.show()

# ── Find changed IDs ──
changed_ids = existing_df.alias("old").join(new_df.alias("new"), "id", "inner") \
    .filter(col("old.city") != col("new.city")) \
    .select("old.id").rdd.flatMap(lambda x: x).collect()

print("Changed IDs:", changed_ids)

# ── Mark old rows inactive ──
updated_df = existing_df \
    .withColumn("is_active", when(col("id").isin(changed_ids), lit(False)).otherwise(col("is_active"))) \
    .withColumn("end_date",  when(col("id").isin(changed_ids), current_date().cast("string")).otherwise(col("end_date")))

print("AFTER MARKING INACTIVE:")
updated_df.show()

# ── New active rows for changed records ──
new_active = new_df.filter(col("id").isin(changed_ids)) \
    .withColumn("is_active",  lit(True)) \
    .withColumn("start_date", current_date().cast("string")) \
    .withColumn("end_date",   lit(None).cast("string"))

print("NEW ACTIVE ROWS:")
new_active.show()

# ── Brand new records ──
existing_ids = existing_df.select("id").rdd.flatMap(lambda x: x).collect()
brand_new = new_df.filter(~col("id").isin(existing_ids)) \
    .withColumn("is_active",  lit(True)) \
    .withColumn("start_date", current_date().cast("string")) \
    .withColumn("end_date",   lit(None).cast("string"))

print("BRAND NEW RECORDS:")
brand_new.show()

# ── Final Result ──
final_df = updated_df.union(new_active).union(brand_new)

print("FINAL RESULT AFTER SCD TYPE 2:")
final_df.orderBy("id", "is_active").show()

spark.stop()