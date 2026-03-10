from pyspark.sql import SparkSession

# Create Spark session (Driver)
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

# Sample data
data = ["hello spark", "hello pyspark", "big data spark"]

# Convert to DataFrame
df = spark.createDataFrame(data, "string").toDF("text")

# Split words
from pyspark.sql.functions import split, explode

words = df.select(explode(split(df.text, " ")).alias("word"))

# Count words
word_count = words.groupBy("word").count()

# Show result
word_count.show()