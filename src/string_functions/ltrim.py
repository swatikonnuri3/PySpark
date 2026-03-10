from pyspark.sql import SparkSession
from pyspark.sql.functions import ltrim
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("ltrim").getOrCreate()
df = create_sales_df(spark)
df.select(ltrim("product")).show()