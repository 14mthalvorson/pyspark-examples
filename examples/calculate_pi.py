"""
Adapted from example listed in pyspark github repo
Basic idea
- Pick random point in 2x2 square
- Ratio of points with distance <=1 from the center is pi/4
- Overall process is highly parallel
"""

from random import random
from operator import add
from pyspark.sql import SparkSession


partitions = 100
calculations_per_partition = 100000
total_calculations = partitions * calculations_per_partition


def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


if __name__ == "__main__":

    # Get or Create the Spark cluster "PythonPi"
    # SparkContext represents a connection to a Spark cluster
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    sc = spark.sparkContext

    a = sc.parallelize(range(1, total_calculations + 1), partitions)

    b = a.map(f)

    in_circle_count = b.reduce(add)

    print("Pi is roughly %f" % (in_circle_count / total_calculations * 4.0))

    spark.stop()
