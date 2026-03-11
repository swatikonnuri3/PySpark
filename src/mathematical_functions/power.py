from pyspark.sql import SparkSession
from pyspark.sql.functions import pow

spark=SparkSession.builder.appName("POWER").getOrCreate()

data=[(2,3),(3,2)]
df=spark.createDataFrame(data,["a","b"])

df.select(pow("a","b")).show()

spark.stop()