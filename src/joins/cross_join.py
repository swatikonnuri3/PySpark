from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CrossJoin").getOrCreate()

data1=[(1,"A"),(2,"B")]
data2=[("HR",),("IT",)]

df1=spark.createDataFrame(data1,["id","name"])
df2=spark.createDataFrame(data2,["dept"])

df1.crossJoin(df2).show()

spark.stop()