

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("UpsertExample")
    .master("local[*]")  # use all CPU cores
    .getOrCreate()
)

# -------------------------------
# Step 2: Create initial DataFrame
# -------------------------------
data = [
    (1, "Swati", 50000),
    (2, "Rahul", 45000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

print("Initial Table Data")
df.show()

# -------------------------------
# Step 3: Save initial data as Parquet
# -------------------------------
parquet_path = "C:/Users/Swati/PycharmProjects/PySpark/data/employees_parquet"
df.write.mode("overwrite").parquet(parquet_path)

# -------------------------------
# Step 4: New data to upsert
# -------------------------------
new_data = [
    (1, "Swati", 60000),   # Existing row to update
    (3, "Anita", 48000)    # New row to insert
]

new_df = spark.createDataFrame(new_data, columns)
print("New Data to Upsert")
new_df.show()

# -------------------------------
# Step 5: Read existing Parquet
# -------------------------------
existing_df = spark.read.parquet(parquet_path)

# -------------------------------
# Step 6: Merge (Upsert) logic
# -------------------------------
# Union the existing and new data, then remove duplicates based on "id"
merged_df = existing_df.union(new_df).dropDuplicates(["id"])

print("Merged Table After Upsert")
merged_df.show()

# -------------------------------
# Step 7: Save merged data back
# -------------------------------
merged_df.write.mode("overwrite").parquet(parquet_path)

# -------------------------------
# Done
# -------------------------------
print(f"Upsert completed. Data saved at: {parquet_path}")

# Stop Spark session
spark.stop()