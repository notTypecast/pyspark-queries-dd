from pyspark.sql import SparkSession


def query(spark, filename):
    df = spark.read.csv(filename, header=True)
    df = (
        df.withColumn("Open", df["Open"].cast("float"))
        .withColumn("Close", df["Close"].cast("float"))
        .withColumn("Volume", df["Volume"].cast("float"))
    )

    df.createOrReplaceTempView("stock")

    df_max_open_year = spark.sql(
        "SELECT YEAR(Date) as Year, Open FROM stock WHERE Open = (SELECT MAX(Open) FROM stock)"
    )
    df_min_close_year = spark.sql(
        "SELECT YEAR(Date) as Year, Close FROM stock WHERE Close = (SELECT MIN(Close) FROM stock)"
    )

    print(filename)
    print("Year with max open price:")
    df_max_open_year.show()
    print("Year with min close price:")
    df_min_close_year.show()


filenames = ["agn.us.txt", "ainv.us.txt", "ale.us.txt"]
spark = SparkSession.builder.appName("Stock").getOrCreate()
for filename in filenames:
    query(spark, filename)
