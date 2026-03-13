import os
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("WriteFormatsExample") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample Data
data = [
    (1, "Swati", 21),
    (2, "Rahul", 22),
    (3, "Anjali", 20)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

base_path = "C:/Users/Swati/PycharmProjects/PySpark/data"

# ── WRITE ──────────────────────────────────────────────
df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base_path}/students_csv")
df.coalesce(1).write.mode("overwrite").json(f"{base_path}/students_json")
df.coalesce(1).write.mode("overwrite").parquet(f"{base_path}/students_parquet")

print("Write complete!")

# ── READ BACK ──────────────────────────────────────────
print("\n--- CSV ---")
spark.read.option("header", True).csv(f"{base_path}/students_csv").show()

print("--- JSON ---")
spark.read.json(f"{base_path}/students_json").show()

print("--- Parquet ---")
spark.read.parquet(f"{base_path}/students_parquet").show()

spark.stop()
