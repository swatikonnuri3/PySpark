from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

df = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

df.writeStream.outputMode("append").format("console").start().awaitTermination()