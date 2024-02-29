from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaPySparkApp") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9093"
kafka_topic = "stock_data"

# Define Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic
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
    .select("data.symbol", "data.date", explode("data.data").alias("metric", "value"))

# Perform transformations
transformed_df = parsed_df.groupBy("symbol", "date").agg(
    sum("value").alias("total_value")
)

# Calculate simple moving average for 50 days and 200 days
transformed_df = transformed_df.withColumn("sma_50_days", avg("total_value").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-49, 0))) \
    .withColumn("sma_200_days", avg("total_value").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-199, 0)))

# Calculate RSI (Relative Strength Index)
window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
change = transformed_df["total_value"] - lag(transformed_df["total_value"]).over(window_spec)
gain = when(change > 0, change).otherwise(0)
loss = when(change < 0, abs(change)).otherwise(0)
avg_gain = avg(gain).over(window_spec)
avg_loss = avg(loss).over(window_spec)
rs = avg_gain / avg_loss
rsi = 100 - (100 / (1 + rs))
transformed_df = transformed_df.withColumn("rsi", when(avg_loss == 0, 100).otherwise(rsi))

# Do weekly, monthly, and yearly time-based aggregation
transformed_df = transformed_df.withColumn("week", weekofyear("date")) \
    .withColumn("month", month("date")) \
    .withColumn("year", year("date")) \
    .groupBy("symbol", "week", "month", "year").agg(
        avg("total_value").alias("avg_total_value"),
        avg("sma_50_days").alias("avg_sma_50_days"),
        avg("sma_200_days").alias("avg_sma_200_days"),
        avg("rsi").alias("avg_rsi")
    )

# Print the transformed data
query = transformed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
