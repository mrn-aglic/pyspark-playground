import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Broadcast logs script ch05 (ex5_5)").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
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

# ex 5.5 table
program_call_signs = spark.read.csv(
    os.path.join(DIRECTORY, "Call_Signs.csv"),
    sep=",",
    header=True,
    inferSchema=True,
).select(
    "LogIdentifierID",
    F.col("Undertaking_Name").alias("undertaking_name"),
)


log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

joined_logs = logs.join(
    log_identifier,
    "LogServiceID",  # more complex logs["LogServiceID"] == log_identifier["LogServiceID"]
    how="inner",
)

joined_logs_with_signs = joined_logs.join(
    program_call_signs, how="inner", on="LogIdentifierID"
)

full_log = joined_logs_with_signs.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

commercial_programs = [
    "COM",
    "PRC",
    "PGI",
    "PRO",
    "PSA",
    "MAG",
    "LOC",
    "SPO",
    "MER",
    "SOL",
]

answer = (
    full_log.groupby("LogIdentifierID", "undertaking_name")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(commercial_programs),
                F.col("duration_seconds"),
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
    )
)

# Fill null values
answer_filled = answer.fillna(0)

answer_filled.orderBy("commercial_ratio", ascending=False).show(1000, False)
