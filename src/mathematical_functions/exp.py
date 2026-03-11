from pyspark.sql import SparkSession
from pyspark.sql.functions import exp

spark=SparkSession.builder.appName("EXP").getOrCreate()

data=[(1,),(2,)]
df=spark.createDataFrame(data,["num"])

df.select(exp("num")).show()

spark.stop()