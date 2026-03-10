from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("FirstSparkApp").getOrCreate()

# Create sample data
data = [("Swati",22), ("Rahul",25), ("Anita",23)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name","Age"])

# Show data
df.show()