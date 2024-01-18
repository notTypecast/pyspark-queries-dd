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

# Get countries with max total value per year
df_max_per_year = spark.sql(
    "SELECT Year, Country, Value FROM tourism t1 WHERE Value = (SELECT MAX(Value) FROM tourism t2 WHERE t1.Year = t2.Year) ORDER BY Year"
)

print("Countries with max total value per year:")
df_max_per_year.show()
