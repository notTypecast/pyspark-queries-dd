from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def query(spark, filename):
    df = spark.read.csv(filename, header=True)
    df = (
        df.withColumn("Open", col("Open").cast("float"))
        .withColumn("Close", col("Close").cast("float"))
        .withColumn("Volume", col("Volume").cast("float"))
    )

    df_35_count = df.filter(df["Open"] > 35).count()
    print(filename)
    print(f"Number of days with open price > 35: {df_35_count}")


filenames = ["agn.us.txt", "ainv.us.txt", "ale.us.txt"]
spark = SparkSession.builder.appName("Stock").getOrCreate()
for filename in filenames:
    query(spark, filename)
