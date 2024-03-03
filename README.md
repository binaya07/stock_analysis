# stock_analysis
Big Data Pipeline for Real-Time Stock Market Analysis

### Steps

1. Install Spark. https://medium.com/@agusmahari/pyspark-step-by-step-guide-to-installing-pyspark-on-linux-bb8af96ea5e8 

2. Run Kafka.
```
docker-compose up 
```

3. Run producer.py to ingest stock market data into Kafka. ['SPY', 'AAPL', 'MSFT', 'TSLA', 'NVDA', 'AMZN', 'GOOG', 'META', 'JPM', 'GME'] stocks data from 2014 are chosen for this project.
```
python producer.py
```

4. Run spark application. It does aggregations and calculates metrics like Moving Averages and RSI. Outputs are stored in parquet files.
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --master local spark-app.py
```

5. Run post_transformer.py that transforms data into a single csv file for easy visualization.
```
spark-submit --master local post_transformer.py
```