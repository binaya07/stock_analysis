from kafka import KafkaProducer
from datetime import datetime
import json
import yfinance as yf
import pandas as pd

# Define the Kafka broker
bootstrap_servers = 'localhost:9093'

# Define the Kafka topic
topic = 'stock_data'

# Define the list of ticker symbols
ticker_symbols = ['SPY', 'AAPL', 'MSFT', 'TSLA', 'NVDA', 'AMZN', 'GOOG', 'META', 'JPM', 'GME']

# Define start and end dates
start_date = '2023-01-01'  # Ten years ago
end_date = '2024-01-01'  # Today

# Fetch historical data for each ticker symbol
for symbol in ticker_symbols:
    data = yf.download(symbol, start=start_date, end=end_date)
    
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    # Produce each row of the DataFrame to the Kafka topic
    for index, row in data.iterrows():
        message = {"symbol": symbol, "date": index.strftime('%Y-%m-%d'), "data": row.to_dict()}
        producer.send(topic, value=message)

# Close the producer
producer.close()
