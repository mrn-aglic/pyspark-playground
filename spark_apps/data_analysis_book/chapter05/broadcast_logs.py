import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Broadcast logs script ch05").getOrCreate()

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

cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

log_identifier.printSchema()

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
print(log_identifier.count())

log_identifier.show(5)

joined_logs = logs.join(
    log_identifier,
    on=[
        "LogServiceID"
    ],  # more complex logs["LogServiceID"] == log_identifier["LogServiceID"]
    how="inner",
)

full_log = joined_logs.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

joined_logs.printSchema()
joined_logs.show(5)
