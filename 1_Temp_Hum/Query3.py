import json
from math import sqrt
from pyspark import SparkContext

sc = SparkContext()

# Load the text file using the SparkContext
file_rdd = sc.textFile("hum.txt")


def parse_row(row):
    try:
        return json.loads(row)
    except json.JSONDecodeError:
        return None


# Cache function return values
def cache_func(func):
    cache = {}

    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]

    return wrapper


# Cache mean function to avoid re-calculating for every day of month (O(n^2) to O(n))
@cache_func
def mean(values):
    return sum(values) / len(values)


parsed_rdd = file_rdd.map(parse_row)

filtered_rdd = parsed_rdd.map(lambda x: {k: v for k, v in x.items() if v})

# Reduce timestamp to month
month_rdd = filtered_rdd.flatMap(lambda x: ((k[:7], float(v)) for k, v in x.items()))
grouped_month_rdd = month_rdd.groupByKey()

std_dev_rdd = grouped_month_rdd.map(
    lambda x: (x[0], sqrt(sum((item - mean(x[1])) ** 2 for item in x[1]) / len(x[1])))
)

print(
    "Month with highest standard deviation: {} at {}".format(
        *std_dev_rdd.max(lambda x: x[1])
    )
)
