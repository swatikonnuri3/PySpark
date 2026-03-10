from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("regex_extract").getOrCreate()
df = create_sales_df(spark)
df.select(regexp_extract("salesperson","\\d+",0)).show()