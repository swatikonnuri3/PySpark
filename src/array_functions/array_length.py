from pyspark.sql import SparkSession
from pyspark.sql.functions import size

spark = SparkSession.builder.appName("ARRAY_LENGTH").getOrCreate()

data=[([1,2,3],),([4,5],)]
df=spark.createDataFrame(data,["numbers"])

df.select("numbers",size("numbers").alias("length")).show()

spark.stop()