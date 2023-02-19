import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch04 - Broadcast logs stats").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs.show(10, False)

logs.printSchema()

for col in logs.columns:
    logs.describe(col).show()

for col in logs.columns:
    logs.select(col).summary().show()

for col in logs.columns:
    logs.select(col).summary("min", "10%", "90%", "max").show()


# WARNING describe() and summary() are two very useful methods,
# but they are not meant to be used for anything other than quickly peeking at data during development.
# The PySpark developers don’t guarantee that the output will look the same from version to version,
# so if you need one of the outputs for your program,
# use the corresponding function in pyspark.sql.functions.
# They’re all there.
