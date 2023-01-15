import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex4_4").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs_clean = logs.select(*[col for col in logs.columns if not col.endswith("ID")])

logs.printSchema()

logs_clean.printSchema()

logs_clean.show(10)
