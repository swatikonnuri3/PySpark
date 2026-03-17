from pyspark.sql import SparkSession
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("full_load").getOrCreate()
sales_df = create_sales_df(spark, 50)
sales_df.write.mode("overwrite").parquet("./full_sales")