from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

# Before running this code: open the socket in terminal on port 9999 with command:
#     $ncat -l 9999

spark = (
    SparkSession
    .builder
    .master("local")
    .appName("pysparkOnJupyterLab")
    .config("spark.sql.shuffle.partitions", 10)
    .getOrCreate()
)

print(spark)

raw_df = spark.readStream.format("socket").option("host","localhost").option("port","9999").load()

raw_df.printSchema()

trans_df = raw_df.withColumn("words", explode(split(lower(col("value")), "\\W+"))) \
    .drop("value") \
    .groupBy("words") \
    .count() \
    .withColumnRenamed("count","word_count")

trans_df.writeStream.format("console").outputMode("complete").start().awaitTermination()