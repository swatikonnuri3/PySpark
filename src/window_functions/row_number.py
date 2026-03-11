from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark=SparkSession.builder.appName("RowNumber").getOrCreate()

data=[("A",100),("A",200),("B",150)]
df=spark.createDataFrame(data,["dept","salary"])

window=Window.partitionBy("dept").orderBy("salary")

df.withColumn("row_number",row_number().over(window)).show()

spark.stop()