from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Show Example").getOrCreate()

data=[("A",1),("B",2),("C",3)]

df=spark.createDataFrame(data,["Letter","Number"])

df.show()