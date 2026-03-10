from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("length").getOrCreate()
df = create_sales_df(spark)
df.select(length("product")).show()