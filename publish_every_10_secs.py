from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import requests
import time

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("KafkaDataPublisher") \
    .getOrCreate()

# Kafka configuration
kafka_params = {
    "bootstrap.servers": "localhost:9092"
}

# Defining the URLs of crypto prices
urls = [
    "http://api.coincap.io/v2/assets/xrp",
    "http://api.coincap.io/v2/assets/bitcoin",
    "http://api.coincap.io/v2/assets/ethereum",
    "http://api.coincap.io/v2/assets/tether",
    "http://api.coincap.io/v2/assets/polygon"
]

#  Defining Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# This function will  fetch data from API and will it publish it to kafka
def fetch_and_publish_data(url, topic, producer):
    try:
        response = requests.get(url) # fetching the data
        response.raise_for_status()  # Raise an error for non-200 response codes
        data = response.json() 
        latest_data = data.get("data", {}) # extracting the json data
        
        # Preparing data payload to be published
        payload = {
            "id": latest_data.get("id", ""),
            "symbol": latest_data.get("symbol", ""),
            "name": latest_data.get("name", ""),
            "marketCapUsd": float(latest_data.get("marketCapUsd", 0)),
            "volumeUsd24Hr": float(latest_data.get("volumeUsd24Hr", 0)),
            "priceUsd": float(latest_data.get("priceUsd", 0)),
            "changePercent24Hr": float(latest_data.get("changePercent24Hr", 0)),
            "timestamp": latest_data.get("timestamp", 0)
        }
        
        # Publish the data
        producer.send(topic, json.dumps(payload).encode('utf-8'))
        print(f"Published data to topic {topic}")
    except Exception as e:
        print(f"Failed to fetch data from {url}. Error: {e}")

# These are the kafka topics to publish
topics = ["xrp_prices", "bitcoin_prices", "ethereum_prices", "tether_prices", "polygon_prices"]

# Implementing a continuous loop to fetch data from URLs and publish to kafka
while True:
    for url, topic in zip(urls, topics):
        fetch_and_publish_data(url, topic, producer) # calling the fetching function
    
    time.sleep(10) # a 10 second delay

# Closing the Kafka producer 
producer.close()
