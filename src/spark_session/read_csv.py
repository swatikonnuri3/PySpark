from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVExample").getOrCreate()

df = spark.read.csv("students.csv", header=True, inferSchema=True)

df.show()