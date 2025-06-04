from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

spark = SparkSession \
    .builder \
    .master("local[*]")\
    .appName("streaming-with-kafka")\
    .config("spark.sql.shuffle.partitions",4)\
    .config("spark.streaming.stopGraceFullyOnShutdown",True)\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")\
    .getOrCreate()

spark.conf.set("spark.sql.streaming.schemaInference",True)

raw_data = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","")\
    .option("subscribe",)\
    .option("")\
    .load()

