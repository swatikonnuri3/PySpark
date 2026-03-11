from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lead

spark=SparkSession.builder.appName("Lead").getOrCreate()

data=[("A",100),("A",200),("A",300)]
df=spark.createDataFrame(data,["dept","salary"])

window=Window.partitionBy("dept").orderBy("salary")

df.withColumn("next_salary",lead("salary").over(window)).show()

spark.stop()