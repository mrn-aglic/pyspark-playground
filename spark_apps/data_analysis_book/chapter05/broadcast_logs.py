import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ch05 - Broadcast logs script ch05").getOrCreate()

DIRECTORY = "/opt/spark/data/broadcast_logs"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs_2018_Q3_M8_sample.CSV"),
    sep="|",
    header=True,
    inferSchema=True,
    timestampFormat="yyyy-MM-dd",
)

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

# log_identifier.printSchema()

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

# print(log_identifier.count())
# log_identifier.show(5)

joined_logs = logs.join(
    log_identifier,
    "LogServiceID",  # more complex logs["LogServiceID"] == log_identifier["LogServiceID"]
    how="inner",
)

full_log = joined_logs.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

# joined_logs.printSchema()
# joined_logs.show(5)

full_log.groupby("ProgramClassCD", "ProgramClass_Description").agg(
    F.sum("duration_seconds").alias("duration_total")
).orderBy("duration_total", ascending=False).show(100, False)

# F.when(
#     F.trim(F.col("ProgramClassCD")).isin(
#         ["COM", "PRC", "PGI", "PRO", "PSA", "MAG", "LOC", "SPO", "MER", "SOL"]
#     ),
#     F.col("duration_seconds") # take this value if the ProgramClassCD value is in list
# ).otherwise(0)

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
    full_log.groupby("ProgramClassCD", "ProgramClass_Description")
    .agg(
        F.sum(
            F.when(
                F.trim(F.col("ProgramClassCD")).isin(commercial_programs),
                F.col(
                    "duration_seconds"
                ),  # take this value if the ProgramClassCD value is in list
            ).otherwise(0)
        ).alias("duration_commercial"),
        F.sum("duration_seconds").alias("duration_total"),
    )
    .withColumn(
        "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
    )
)

answer.orderBy("commercial_ratio", ascending=False).show(1000, False)

# Drop null values
answer_no_null = answer.dropna(subset=["commercial_ratio"])

answer_no_null.orderBy("commercial_ratio", ascending=False).show(1000, False)

# Fill null values
answer_filled = answer.fillna(0)

answer_filled.orderBy("commercial_ratio", ascending=False).show(1000, False)
