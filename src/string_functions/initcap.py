from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("initcap").getOrCreate()
df = create_sales_df(spark)
df.select(initcap("product")).show()