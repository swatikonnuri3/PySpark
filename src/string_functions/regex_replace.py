from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("regex_replace").getOrCreate()
df = create_sales_df(spark)
df.select(regexp_replace("city","a","@")).show()