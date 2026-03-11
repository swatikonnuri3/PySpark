from pyspark.sql import SparkSession
from pyspark.sql.functions import ceil

spark=SparkSession.builder.appName("CEIL").getOrCreate()

data=[(10.2,),(5.7,)]
df=spark.createDataFrame(data,["num"])

df.select(ceil("num")).show()

spark.stop()