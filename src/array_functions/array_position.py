from pyspark.sql import SparkSession
from pyspark.sql.functions import array_position

spark = SparkSession.builder.appName("ARRAY_POSITION").getOrCreate()

data=[([1,2,3],),([4,5,6],)]
df=spark.createDataFrame(data,["numbers"])

df.select("numbers",array_position("numbers",2).alias("position")).show()

spark.stop()