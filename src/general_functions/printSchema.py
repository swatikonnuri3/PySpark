from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("print_schema").getOrCreate()
data = [("aaa",25),("bbb",30)]
df = spark.createDataFrame(data,["Name","Age"])
df.printSchema()
spark.stop()