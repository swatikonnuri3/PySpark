from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OuterJoin").getOrCreate()

data1=[(1,"Alice"),(2,"Bob")]
data2=[(2,"IT"),(3,"HR")]

df1=spark.createDataFrame(data1,["id","name"])
df2=spark.createDataFrame(data2,["id","dept"])

df1.join(df2,"id","outer").show()

spark.stop()