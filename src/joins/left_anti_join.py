from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("LeftAntiJoin").getOrCreate()

data1=[(1,"Alice"),(2,"Bob"),(3,"Charlie")]
data2=[(2,"IT")]

df1=spark.createDataFrame(data1,["id","name"])
df2=spark.createDataFrame(data2,["id","dept"])

df1.join(df2,"id","left_anti").show()

spark.stop()