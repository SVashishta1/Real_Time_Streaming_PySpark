from pyspark.sql import SparkSession
from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import json
import requests
import pymysql
import os

# This is to Setup the PYSPARK_SUBMIT_ARGS environment variable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

# MySQL database configuration
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'RootPass@#007'
DB_NAME = 'pyspark_project'

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Kafka configuration
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "xrp_prices,bitcoin_prices,ethereum_prices,tether_prices,polygon_prices"
}

# Defining the schema for the incoming JSON data
schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("name", StringType()) \
    .add("marketCapUsd", DoubleType()) \
    .add("volumeUsd24Hr", DoubleType()) \
    .add("priceUsd", DoubleType()) \
    .add("changePercent24Hr", DoubleType()) \
    .add("timestamp", TimestampType())



# Kafka topics 
topics = ["xrp_prices", "bitcoin_prices", "ethereum_prices", "tether_prices", "polygon_prices"]



# This function will store data in MySQL database
def store_in_mysql(data_df, batch_id):
    print(f"Received batch {batch_id} with {data_df.count()} records")
    
    # Check for an empty DataFrame 
    if data_df.isEmpty():
        print(f"No records in batch {batch_id}. Skipping processing.")
        return
    
    # Printing the DataFrame for debugging
    print("Schema of crypto_df:")
    data_df.printSchema()
    print("Content of crypto_df:")
    data_df.show(truncate=False)
    
    pd_df = data_df.toPandas()
    
    #  Connecting to MySQL database
    try:
        connection = pymysql.connect(host=DB_HOST,
                                     user=DB_USER,
                                     password=DB_PASSWORD,
                                     database=DB_NAME,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        
        with connection.cursor() as cursor:
            # Inserting required data into MySQL db table
            for row in data_df.collect():
                sql = "INSERT INTO crypto_prices (id, symbol, name, marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr ) VALUES (%s, %s, %s, ROUND(%s, 3), ROUND(%s, 3), ROUND(%s, 3), ROUND(%s, 3))"
                cursor.execute(sql, (row['id'], row['symbol'], row['name'], row['marketCapUsd'], row['volumeUsd24Hr'],
                                     row['priceUsd'], row['changePercent24Hr']))
        
        connection.commit()
        print(f"Stored batch {batch_id} in MySQL database.")
    except Exception as e:
        print(f"Failed to store batch {batch_id} in MySQL database. Error: {e}")
        # Printing detailed error message from MySQL
        if 'cursor' in locals() and cursor:
            print(f"MySQL Error: {cursor._last_executed}")
    finally:
        if connection:
            connection.close()

# Creating a DStream 
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Converting the kafka input data to DataFrame
crypto_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Processing each RDD in the DStream
query = crypto_df.writeStream \
    .foreachBatch(store_in_mysql) \
    .start()


query.awaitTermination()
