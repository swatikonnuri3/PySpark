from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MLExample").getOrCreate()

data = [(1,10),(2,20),(3,30),(4,40)]

df = spark.createDataFrame(data, ["feature","label"])

assembler = VectorAssembler(inputCols=["feature"], outputCol="features")

df2 = assembler.transform(df)

lr = LinearRegression(featuresCol="features", labelCol="label")

model = lr.fit(df2)

model.summary.predictions.show()