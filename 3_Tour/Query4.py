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

"""
ranked table: Sorts each year based on Value and assigns a rank to each country
We then select, for each country, the year(s) where the rank is the lowest
"""
df_ranked = spark.sql(
    """
    WITH ranked AS (
        SELECT Country, Year, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Value DESC) Rank 
        FROM tourism
    ) 
    SELECT r.Country, r.Year, r.Rank
    FROM ranked AS r
    INNER JOIN (
        SELECT Country, MAX(Rank) AS LOWEST_RANK
        FROM ranked
        GROUP BY Country 
    ) AS r2
    ON r.Country = r2.Country AND r.Rank = r2.LOWEST_RANK
    ORDER BY r.Country
    """
)

df_ranked.show(200)
