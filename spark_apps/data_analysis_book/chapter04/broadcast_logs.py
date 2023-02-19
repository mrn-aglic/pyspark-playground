import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch04 -  Broadcast logs").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs = logs.select("BroadcastLogID", "LogServiceID", "LogDate")

logs.show(10, False)

logs.printSchema()
