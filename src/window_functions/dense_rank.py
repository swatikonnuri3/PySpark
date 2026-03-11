from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

spark=SparkSession.builder.appName("DenseRank").getOrCreate()

data=[("A",100),("A",200),("A",200)]
df=spark.createDataFrame(data,["dept","salary"])

window=Window.partitionBy("dept").orderBy("salary")

df.withColumn("dense_rank",dense_rank().over(window)).show()

spark.stop()