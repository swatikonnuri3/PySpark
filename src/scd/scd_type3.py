import os
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"
os.environ["PATH"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("SCD_Type3").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ── Existing Data ──
existing_schema = StructType([
    StructField("id",            IntegerType(), True),
    StructField("name",          StringType(),  True),
    StructField("current_city",  StringType(),  True),
    StructField("previous_city", StringType(),  True)
])

existing_df = spark.createDataFrame([
    (1, "Swati",  "Mumbai",  None),
    (2, "Rahul",  "Delhi",   None),
    (3, "Anjali", "Chennai", None)
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

# ── Join existing with new ──
joined_df = existing_df.alias("old").join(
    new_df.alias("new"),
    on="id",
    how="full_outer"
)

# ── Apply SCD Type 3 Logic ──
scd3_df = joined_df.select(
    # id
    when(col("new.id").isNotNull(), col("new.id"))
    .otherwise(col("old.id")).alias("id"),

    # name
    when(col("new.name").isNotNull(), col("new.name"))
    .otherwise(col("old.name")).alias("name"),

    # current_city → always take new city
    when(col("new.city").isNotNull(), col("new.city"))
    .otherwise(col("old.current_city")).alias("current_city"),

    # previous_city → store old city only if city changed
    when(
        col("new.city").isNotNull() & (col("new.city") != col("old.current_city")),
        col("old.current_city")
    ).otherwise(col("old.previous_city")).alias("previous_city")
)

print("FINAL RESULT AFTER SCD TYPE 3:")
scd3_df.orderBy("id").show()

spark.stop()