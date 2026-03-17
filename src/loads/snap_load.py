from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from src.dataframes.df_creation.common_df import create_sales_df
spark = SparkSession.builder.appName("snapshot_load").getOrCreate()
sales_df = create_sales_df(spark, 50)
snapshot_df = sales_df.withColumn("snapshot_date", current_date())
snapshot_df.write.mode("append").parquet("./snapshot_sales")
snapshot_df.show()