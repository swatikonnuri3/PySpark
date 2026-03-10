from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("max").getOrCreate()
df = create_sales_df(spark)
df.select(max("price")).show()