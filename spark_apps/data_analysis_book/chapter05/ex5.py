# 5.1. Duplicate all of the rows that do not satisfy join on right table
# 5.2. Inner
# 5.3. Left
# 5.4
# write alternative source code without using left_anti:
# left.join(right, how="left_anti",
#      on="my_column").select("my_column").distinct()

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ex5_4").getOrCreate()

left = [
    ["Banana", 2, 1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99],
]

right = [["Banana"], ["Carrot"]]

left = spark.createDataFrame(left, ["Item", "Quantity", "Price"])
right = spark.createDataFrame(right, ["Item"])

left.join(right, how="left_anti", on="Item").select("Item").distinct().show()

left.alias("left").join(
    right.alias("right"), how="left", on=[right["Item"] == left["Item"]]
).where(F.col("right.Item").isNull()).select(F.col("left.Item")).distinct().show()

# official solution
left.join(right, how="left", on=left["Item"] == right["Item"]).where(
    right["Item"].isnull()
).select(left["Item"])
