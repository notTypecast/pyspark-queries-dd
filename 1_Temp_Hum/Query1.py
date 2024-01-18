import json
from pyspark import SparkContext

sc = SparkContext()

# Load the text file using the SparkContext
file_rdd = sc.textFile("tempm.txt")


def parse_row(row):
    try:
        return json.loads(row)
    except json.JSONDecodeError:
        return None


parsed_rdd = file_rdd.map(parse_row)

filtered_rdd = parsed_rdd.map(lambda x: {k: v for k, v in x.items() if v})

# Find out how many days had temperatures in [18, 22]
filtered_range_rdd = filtered_rdd.filter(
    lambda x: all(18 <= float(x[key]) <= 22 for key in x)
)
print(f"Number of days with temperatures in [18, 22]: {filtered_range_rdd.count()}")
