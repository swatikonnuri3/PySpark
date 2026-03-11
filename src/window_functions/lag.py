from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

spark=SparkSession.builder.appName("Lag").getOrCreate()

data=[("A",100),("A",200),("A",300)]
df=spark.createDataFrame(data,["dept","salary"])

window=Window.partitionBy("dept").orderBy("salary")

df.withColumn("prev_salary",lag("salary").over(window)).show()

spark.stop()