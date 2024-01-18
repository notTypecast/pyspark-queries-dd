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

# Comparing Greece to Spain, France, Italy, Germany and Norway
other_countries = ["Spain", "France", "Italy", "Sweden", "Norway"]

# Get years for which Greece has a higher value than each other country
for country in other_countries:
    df_years = spark.sql(
        "SELECT t1.Year FROM tourism t1 LEFT JOIN tourism t2 ON t1.Year = t2.Year WHERE t1.Country = 'Greece' AND t2.Country = '{}' AND t1.Value > t2.Value ORDER BY t1.Year".format(
            country
        )
    )
    print(f"Years for which Greece has a higher value than {country}: ")
    df_years.show()
