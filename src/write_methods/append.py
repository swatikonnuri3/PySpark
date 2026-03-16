# append_example.py
import os
import sys

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]
from pyspark.sql import SparkSession

# -------------------------------
# Step 1: Create SparkSession
# -------------------------------
spark = (
    SparkSession.builder
    .appName("AppendExample")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------------
# Step 2: Existing data
# -------------------------------
data = [
    (1, "Swati", 60000),
    (2, "Rahul", 45000),
    (3, "Anita", 48000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

# Save as Parquet if not already saved
parquet_path = "C:/Users/Swati/PycharmProjects/PySpark/data/employees_parquet"
df.write.mode("overwrite").parquet(parquet_path)

print("Existing Table Data:")
df.show()

# -------------------------------
# Step 3: New data to append
# -------------------------------
new_data = [
    (4, "Karan", 52000),
    (5, "Priya", 47000)
]

new_df = spark.createDataFrame(new_data, columns)
print("New Data to Append:")
new_df.show()

# -------------------------------
# Step 4: Append new data
# -------------------------------
new_df.write.mode("append").parquet(parquet_path)

# -------------------------------
# Step 5: Read back merged data
# -------------------------------
merged_df = spark.read.parquet(parquet_path)
print("Table After Append:")
merged_df.show()

# Stop Spark session
spark.stop()