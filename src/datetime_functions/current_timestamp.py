from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("current_timestamp").getOrCreate()
df = create_sales_df(spark)
df.select(current_timestamp()).show()