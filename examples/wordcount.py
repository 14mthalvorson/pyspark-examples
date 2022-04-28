"""
Counts the number of instances of each word in names.txt
Adapted from the examples list in pyspark github repo

"""

from operator import add
from pyspark.sql import SparkSession


if __name__ == "__main__":

    file = "names.txt"

    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    a = spark.read.text(file)  # text file -> dataframe

    b = a.rdd  # dataframe -> RDD (Resilient Distributed Dataset, immutable)

    c = b.map(lambda r: r[0])  # creates a list of just the lines in the file

    d = c.flatMap(lambda x: x.split(' '))  # Split lines by spaces and flatten each word to its own line

    e = d.map(lambda x: (x, 1))  # Creates a tuple (word, 1)

    f = e.reduceByKey(add)  # Combines instances of the same word, adds counts

    g = f.collect()  # RDD -> List

    g.sort(key=lambda x:x[1], reverse=True)

    for (word, count) in g:
        print("%s: %i" % (word, count))

    spark.stop()
