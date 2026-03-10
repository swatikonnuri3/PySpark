from pyspark.sql import SparkSession
from pyspark.sql.functions import abs
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("abs").getOrCreate()
df = create_sales_df(spark)
df.select(abs("price")).show()