from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaPySparkApp") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9093"
kafka_topic = "stocks"

# Define Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest"
}

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()


# Define schema for the incoming data
schema = StructType([
    StructField("symbol", StringType()),
    StructField("date", StringType()),
    StructField("data", MapType(StringType(), DoubleType()))
])

# Parse JSON data from Kafka and apply schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.symbol", "data.date", 
            col("data.data").getItem("Open").alias("open"),
            col("data.data").getItem("Close").alias("close"),
            col("data.data").getItem("High").alias("high"),
            col("data.data").getItem("Low").alias("low"),
            col("data.data").getItem("Volume").alias("volume"))

parsed_df_with_col = parsed_df.withColumn("timestamp", to_timestamp("date"))


# Define watermark to handle late arriving data (1 day in this example)
watermark_duration = "1 day"
parsed_df_with_watermark = parsed_df_with_col.withWatermark("timestamp", watermark_duration)

# Define a sliding window of 50 days
window_duration = "50 days"
slide_duration = "1 day"

# Calculate the 50-day moving average for each symbol within the sliding window
moving_avg_df = parsed_df_with_watermark.groupBy("symbol", window("timestamp", window_duration, slide_duration)) \
    .agg(avg("close").alias("moving_avg_50"))
moving_avg_df_f = moving_avg_df.select("symbol", to_date("window.end").alias("date"), "moving_avg_50")

# Define the output directory for the Parquet files
sma_output_directory = "parquet/sma"
sma_checkpoint_location = "parquet/sma/checkpoint"
# Start the streaming query to write results to Parquet files
query = moving_avg_df_f.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", sma_output_directory) \
    .option("checkpointLocation", sma_checkpoint_location) \
    .start()
# Wait for the query to terminate
query.awaitTermination()
