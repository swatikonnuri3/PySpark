from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("read_parquet").getOrCreate()
df=spark.read.parquet("../../users.parquet")
df.printSchema()
df.show()