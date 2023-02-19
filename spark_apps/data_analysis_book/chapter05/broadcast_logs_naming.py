import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch05 - Broadcast logs script naming ch05").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier.printSchema()

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
print(log_identifier.count())

log_identifier.show(5)

# 1st approach to handling column ambiguity - use the short syntax for join type, e.g. on="LogServiceID"

# 2nd drop one of the columns
joined_logs = logs.join(
    log_identifier,
    on=[logs["LogServiceID"] == log_identifier["LogServiceID"]],
)

joined_logs = joined_logs.drop(log_identifier["LogServiceID"])

# 3rd alias the table
joined_logs = logs.join(
    log_identifier.alias("right"),
    logs["LogServiceID"] == log_identifier["LogServiceID"],
)

joined_logs = joined_logs.drop(F.col("right.LogServiceID")).select("LogServiceID")

joined_logs.printSchema()
joined_logs.show(5)
