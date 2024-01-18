import json
from math import sqrt
from pyspark import SparkContext

sc = SparkContext()

file_rdd_temp = sc.textFile("tempm.txt")
file_rdd_hum = sc.textFile("hum.txt")


def parse_row(row):
    try:
        return json.loads(row)
    except json.JSONDecodeError:
        return None


parsed_rdd_temp = file_rdd_temp.map(parse_row)
filtered_rdd_temp = parsed_rdd_temp.map(lambda x: {k: v for k, v in x.items() if v})
flat_rdd_temp = filtered_rdd_temp.flatMap(
    lambda x: ((k, float(v)) for k, v in x.items())
)

parsed_rdd_hum = file_rdd_hum.map(parse_row)
filtered_rdd_hum = parsed_rdd_hum.map(lambda x: {k: v for k, v in x.items() if v})
flat_rdd_hum = filtered_rdd_hum.flatMap(lambda x: ((k, float(v)) for k, v in x.items()))

DI_rdd = flat_rdd_temp.union(flat_rdd_hum).reduceByKey(
    lambda T, H: T - 0.55 * (1 - 0.01 * H) * (T - 14.5)
)

print(f"Maximum DI value: {DI_rdd.max(lambda x: x[1])[1]:.2f}")
print(f"Minimum DI value: {DI_rdd.min(lambda x: x[1])[1]:.2f}")
