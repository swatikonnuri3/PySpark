from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("CAST").getOrCreate()

data=[("10",),("20",)]
df=spark.createDataFrame(data,["num"])

df.select(df.num.cast("int")).show()

spark.stop()