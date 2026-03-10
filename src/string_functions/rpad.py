from pyspark.sql import SparkSession
from pyspark.sql.functions import rpad
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("rpad").getOrCreate()
df = create_sales_df(spark)
df.select(rpad("product",12,"*")).show()