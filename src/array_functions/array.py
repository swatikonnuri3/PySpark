from pyspark.sql import SparkSession
from pyspark.sql.functions import array

spark = SparkSession.builder.appName("ARRAY").getOrCreate()

data=[(1,2),(3,4),(5,6)]
df=spark.createDataFrame(data,["col1","col2"])

df.select(array("col1","col2").alias("array_column")).show()

spark.stop()