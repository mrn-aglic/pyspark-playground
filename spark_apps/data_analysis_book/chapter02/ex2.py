# ex 2.3

# Rewrite:
# exo2_3_df = (
#     spark.read.text("./data/gutenberg_books/1342-0.txt")
#     .select(length(col("value")))
#     .withColumnRenamed("length(value)", "number_of_char")
# )
#
# Solution:
# exo2_3_df = (
#     spark.read.text("./data/gutenberg_books/1342-0.txt")
#     .select(length(col("value")).alias("number_of_char"))
# )

# ex 2.5
# a)
# words_without_is = words_nonull.where(col("word") != "is")
# b)
# words_more_than_3_char = words_nonull.where(length(col("word")) > 3)

# ex 2.6
# words_no_is_not_the_if = (
#     words_nonull.where(~col("word").isin(
#         ["no", "is", "the", "if"])))
