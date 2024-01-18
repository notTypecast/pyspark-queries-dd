from pyspark.sql import SparkSession


def query(spark, filename):
    df = spark.read.csv(filename, header=True)
    df = (
        df.withColumn("Open", df["Open"].cast("float"))
        .withColumn("Close", df["Close"].cast("float"))
        .withColumn("Volume", df["Volume"].cast("float"))
    )

    df.createOrReplaceTempView("stock")

    df_max_open = spark.sql(
        "SELECT Date, Open FROM stock WHERE Open = (SELECT MAX(Open) FROM stock)"
    )
    df_max_volume = spark.sql(
        "SELECT Date, Volume FROM stock WHERE Volume = (SELECT MAX(Volume) FROM stock)"
    )

    print(filename)
    print("Days with max open price:")
    df_max_open.show()
    print("Days with max volume:")
    df_max_volume.show()


filenames = ["agn.us.txt", "ainv.us.txt", "ale.us.txt"]
spark = SparkSession.builder.appName("Stock").getOrCreate()
for filename in filenames:
    query(spark, filename)
