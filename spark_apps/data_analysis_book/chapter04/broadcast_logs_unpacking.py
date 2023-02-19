import os

import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch04 - Broadcast logs unpacking").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

column_split = np.array_split(np.array(logs.columns), len(logs.columns) // 3)

print(column_split)

for x in column_split:
    logs.select(*x).show(5, False)
