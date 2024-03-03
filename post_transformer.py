from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("ReadParquet") \
    .getOrCreate()

# Define the path to the Parquet files
parquet_path = "parquet/sma_200"
 
# Read data from Parquet files into a DataFrame
df = spark.read.parquet(parquet_path)

# Sort the DataFrame by stock and date
sorted_df = df.orderBy("symbol", "date")

# Define the path to the output CSV file
output_csv_path = "csv/sma_200.csv"

# Write the sorted DataFrame to a single CSV file
sorted_df.write.csv(output_csv_path, header=True)
