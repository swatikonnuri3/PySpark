from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("current_date").getOrCreate()
df = create_sales_df(spark)
df.select(current_date()).show()