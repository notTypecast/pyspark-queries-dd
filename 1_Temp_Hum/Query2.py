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

# Find the 10 coldest and 10 hottest days
avg_temp_rdd = filtered_rdd.map(
    lambda x: (sum(float(v) for v in x.values()) / len(x), next(iter(x.keys()))[:10])
)
avg_temp_rdd = avg_temp_rdd.sortBy(lambda x: x[0])

print(
    "10 coldest days:\n",
    *(
        f"{i+1}. {x[1]} at average {x[0]:.2f}°C\n"
        for i, x in enumerate(avg_temp_rdd.take(10))
    ),
)

print(
    "10 hottest days:\n",
    *(
        f"{i+1}. {x[1]} at average {x[0]:.2f}°C\n"
        for i, x in enumerate(avg_temp_rdd.takeOrdered(10, key=lambda x: -x[0]))
    ),
)
