from pyspark.sql import SparkSession


if __name__ == "__main__":
    file = "numbers.txt"

    spark = SparkSession.builder.appName("PythonSort").getOrCreate()

    a = spark.read.text(file)
    b = a.rdd
    c = b.map(lambda r: r[0])
    d = c.flatMap(lambda x: x.split(' '))
    e = d.map(lambda x: (int(x), 1))
    f = e.sortByKey()
    g = f.collect()

    for (num, count) in g:
        print(num)

    spark.stop()
