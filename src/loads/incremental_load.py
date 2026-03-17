from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("incremental_load").getOrCreate()
existing_sales = create_sales_df(spark, 50)
incoming_sales = create_sales_df(spark, 70)
new_sales = incoming_sales.filter(col("sale_id") > 50)
final_sales = existing_sales.union(new_sales)
final_sales.show()