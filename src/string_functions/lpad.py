from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("lpad").getOrCreate()
df = create_sales_df(spark)
df.select(lpad("product",12,"*")).show()