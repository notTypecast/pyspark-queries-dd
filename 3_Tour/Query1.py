from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pandas as pd

spark = SparkSession.builder.appName("Stock").getOrCreate()

pandas_df = pd.read_excel("tour_occ_ninat.xlsx", header=8)
melted_df = pd.melt(
    pandas_df, id_vars=["GEO/TIME"], var_name="Year", value_name="Value"
)
df = spark.createDataFrame(melted_df)
df.withColumnRenamed("GEO/TIME", "Country").withColumn(
    "Value", when(col("Value") == ":", 0).otherwise(col("Value"))
).withColumn("Value", col("Value").cast("float")).createOrReplaceTempView("tourism")

avg_per_country = spark.sql(
    "SELECT Country, AVG(Value) FROM tourism WHERE Year >= 2007 AND Year <= 2014 GROUP BY Country"
)
avg_per_country.show()
