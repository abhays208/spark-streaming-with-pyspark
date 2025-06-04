from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode

spark = SparkSession \
    .builder\
    .master("local[*]")\
    .appName("stream-process-files")\
    .config("spark.streaming.stopGraceFullyOnShutdown", True)\
    .getOrCreate()

# .config("spark.streaming.stopGraceFullyOnShutdown", True) >> Shuts down the streaming session gracefully on shutdown
# .In order to make spark read the schema on run time you have to set up this configuration
spark.conf.set("spark.sql.streaming.schemaInference", True)

data = spark.readStream\
    .format("json")\
    .option("cleanSource","archive")\
    .option("sourceArchiveDir","../../resources/datasets/archive_dir")\
    .option("maxFilesPerTrigger",1)\
    .load("../../resources/datasets/devices/")

data.printSchema()

data_df = data.withColumn("data_devices", explode(col("data.devices")))

flattened_df = data_df\
    .withColumn("deviceId", col("data_devices.deviceId"))\
    .withColumn("temperature", col("data_devices.temperature"))\
    .withColumn("measure", col("data_devices.measure"))\
    .withColumn("status", col("data_devices.status"))\
    .drop(col("data"))\
    .drop(col("data_devices"))

flattened_df.printSchema()

#Write to console
# flattened_df.writeStream\
#     .format("console")\
#     .outputMode("append")\
#     .start()\
#     .awaitTermination()

#Write to csv file
flattened_df.writeStream\
    .format("csv")\
    .outputMode("append")\
    .option("path","../../resources/datasets/output/device_data")\
    .option("checkpointLocation", "../../resources/checkpoint_dir")\
    .start()\
    .awaitTermination()