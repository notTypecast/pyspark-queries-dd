from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def query(spark, filename):
    df = spark.read.csv(filename, header=True)

    # add month column
    df = (
        df.withColumn("month", df["Date"][0:7])
        .withColumn("Open", col("Open").cast("float"))
        .withColumn("Close", col("Close").cast("float"))
        .withColumn("Volume", col("Volume").cast("float"))
        .sort("month")
    )

    # Get average open price for each month
    avg_open = df.groupBy("month").avg("Open")

    # Get average close price for each month
    avg_close = df.groupBy("month").avg("Close")

    # Get average volume for each month
    avg_volume = df.groupBy("month").avg("Volume")

    print(filename)
    print("Average open price for each month:")
    avg_open.show()
    print("Average close price for each month:")
    avg_close.show()
    print("Average volume for each month:")
    avg_volume.show()


filenames = ["agn.us.txt", "ainv.us.txt", "ale.us.txt"]
spark = SparkSession.builder.appName("Stock").getOrCreate()
for filename in filenames:
    query(spark, filename)
