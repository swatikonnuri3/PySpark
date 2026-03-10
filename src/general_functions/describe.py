from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("describe").getOrCreate()
df = create_sales_df(spark)
df.describe().show()