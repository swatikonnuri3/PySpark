from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Select Example").getOrCreate()

data=[("Swati",22),("Ravi",24)]

df=spark.createDataFrame(data,["Name","Age"])

df.select("Name").show()