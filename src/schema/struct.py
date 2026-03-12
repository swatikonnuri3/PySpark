from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
spark=SparkSession.builder.appName("structs").getOrCreate()
data=[\
    (1,("swati","konnuri"),"Dev"), \
    (2,("fname","lname"),"Tester"),\
    ]
name_schema=StructType(
    [
        StructField("firstName",StringType()),
        StructField("lastName",StringType())

    ]
)
schema=StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",name_schema),
        StructField("position",StringType())
    ]
)

df=spark.createDataFrame(data,schema)
df.show()
df.printSchema()