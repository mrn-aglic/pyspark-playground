import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex4_1").getOrCreate()

DIRECTORY = "/opt/spark/data/"

items = spark.read.csv(
    os.path.join(DIRECTORY, "sample.csv"),
    sep=",",
    header=True,
    inferSchema=True,
    quote="$",
)

items.show()
