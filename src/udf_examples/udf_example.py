from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [("john",), ("anna",), ("mike",)]
df = spark.createDataFrame(data, ["name"])

df.createOrReplaceTempView("people")

def to_upper(name):
    return name.upper()

# Register UDF
spark.udf.register("to_upper_udf", to_upper, StringType())

result = spark.sql("""
SELECT name, to_upper_udf(name) as upper_name
FROM people
""")

result.show()