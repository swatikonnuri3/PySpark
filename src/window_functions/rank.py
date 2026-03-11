from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

spark=SparkSession.builder.appName("Rank").getOrCreate()

data=[("A",100),("A",200),("A",200)]
df=spark.createDataFrame(data,["dept","salary"])

window=Window.partitionBy("dept").orderBy("salary")

df.withColumn("rank",rank().over(window)).show()

spark.stop()