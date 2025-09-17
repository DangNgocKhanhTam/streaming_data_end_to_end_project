# Real-Time Data Pipeline with Kafka, Spark, and Airflow
## **Overview**
This project involves designing a real-time data pipeline with Kafka and Spark Streaming. Stock price data is streamed from a custom Market API and stored in MongoDB. A daily workflow, managed with Apache Airflow, extracts details about stock prices and loads them into a PostgreSQL database. Using this daily data, I produce a concise report highlighting key stock statistics.
 
![1_uCjBWqrRrCL-bTu5HVMySg](https://github.com/user-attachments/assets/7e911c37-cac6-495d-9c90-08436a9cf206)


## Core Components: ##

- **Stock Prices API** : Provides live stock information such as volume, percentage, price, and more.
- **Kafka & Spark Streaming** : Handle real-time data ingestion and processing.
- **MongoDB**: Serves as the storage for streaming stock price data.
- **PostgreSQL**: Archives daily summaries of stocks.
- **Airflow**: Orchestrates the entire ETL workflow, including data extraction, transformation, and reporting.
- **Reporting**: Automates the generation of daily CSV reports with relevant stock metrics.


## Market API ##
You can see a snapshot of the web application output below.

![1_cuZibDc6GwUesEz44fPn9w](https://github.com/user-attachments/assets/12ca73a6-7320-46d0-979f-52e5d537349d)

Before building the API, the first step was to create the necessary SQL tables and populate them with mock data. This was essential because the API needed to fetch certain information directly from our database.

Below, you can see the key SQL tables used in the project. For the sake of clarity, I’ve shortened some of the longer code blocks here — but you can always find the complete versions on my GitHub repository

sql_tables.sql
<pre> -- Sector table
CREATE TABLE sectors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Industry table
CREATE TABLE industries (
    id SERIAL PRIMARY KEY,
    sector_id INTEGER REFERENCES sectors(id),
    name TEXT NOT NULL,
    UNIQUE (sector_id, name)
);

-- Exchange table
CREATE TABLE exchanges (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    code TEXT UNIQUE
);

-- Companies table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector_id INTEGER REFERENCES sectors(id),
    industry_id INTEGER REFERENCES industries(id),
    exchange_id INTEGER REFERENCES exchanges(id),
    country TEXT,
    founded_year INTEGER
);

-- Prices table
CREATE TABLE prices (
    company_id INTEGER REFERENCES companies(id),
    date DATE NOT NULL,
    open NUMERIC(10,2),
    high NUMERIC(10,2),
    low NUMERIC(10,2),
    close NUMERIC(10,2),
    volume BIGINT,
    PRIMARY KEY (company_id, date)
); </pre>

**insert_fake_stock_data**
<pre>INSERT INTO sectors (name) VALUES
('Technology'),
('Healthcare'),
('Financials'),
('Energy'),
('Consumer Goods'),
('Utilities'),
...


INSERT INTO industries (id, sector_id, name) VALUES
(1, 1, 'Television production assistant'),
(2, 1, 'Customer service manager'),
(3, 2, 'Exhibition designer'),
(4, 3, 'Psychologist, clinical'),
(5, 4, 'Forest/woodland manager'),
(6, 4, 'Psychiatric nurse'),
(7, 5, 'Freight forwarder'),
(8, 5, 'Acupuncturist'),
(9, 6, 'Psychologist, occupational'),
(10, 7, 'Holiday representative'),
....


INSERT INTO exchanges (name, code) VALUES
('NASDAQ Stock Market', 'NASDAQ'),
('New York Stock Exchange', 'NYSE'),
('London Stock Exchange', 'LSE'),
('Euronext', 'EURONEXT'),
('Tokyo Stock Exchange', 'TSE'),
...




INSERT INTO companies (id, symbol, name, sector_id, industry_id, exchange_id, country, founded_year) VALUES
(1, 'FTRW', 'Haney-Phillips', 20, 30, 3, 'Brazil', 1967),
(2, 'UVUO', 'Foster, Smith and Grimes', 18, 28, 3, 'Japan', 1987),
(3, 'HGWZ', 'Beltran, Lozano and Mcgee', 4, 5, 7, 'USA', 1985),
(4, 'WDWG', 'Parsons-Hall', 11, 16, 7, 'Australia', 1968),
(5, 'PGKL', 'Robinson-Lee', 3, 4, 5, 'Japan', 2008),
(6, 'YIAU', 'Byrd-Le', 3, 4, 6, 'Canada', 2018),
(7, 'KZVM', 'Becker, Taylor and Davis', 11, 16, 6, 'India', 1966),
(8, 'OMIR', 'Lucas, Parker and Alexander', 2, 3, 2, 'Australia', 1952),
(9, 'DWLA', 'Diaz, Anderson and Browning', 14, 20, 3, 'UK', 1970),
(10, 'WWCJ', 'Burgess-Thompson', 3, 4, 3, 'USA', 2000),
...</pre> 

Now that our SQL tables are set up and populated with sample data, we can go ahead and run the API. You can access the API through this link, after you have initiated the docker environment:
*http://localhost:5000/api/market*

Here’s what the API output will look like:
<pre>[
  {
    "current_price": 123.89,
    "id": 1,
    "name": "Haney-Phillips",
    "open": 123.35,
    "percent_change": 0.44,
    "symbol": "FTRW",
    "timestamp": "2025-07-17 20:32:52",
    "volume": 3486197.97
  }]</pre>

  ## 🛠 Docker Configuration ##

  To manage our services and maintain smooth integration between different components, we’ll use Docker Compose together with a custom Dockerfile. This environment enables us to run Apache Airflow with built-in Java support — a key requirement for working with both Spark and Kafka. It also runs the api, once we have run docker-compose up — build , it starts the API.
  
**Dockerfile**
<pre>FROM apache/airflow:2.6.0-python3.9

COPY requirements.txt /tmp/requirements.txt

USER root

# Java 17 
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# airflow 
USER airflow

# Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["python", "exchange.py"]</pre>

**docker-compose.yaml**
<pre>services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - confluent

  pgadmin:
    image: dpage/pgadmin4
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@admin.com
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "5050:80"
    depends_on:
      - postgres
    networks:
    - confluent
  flask-api:
    build:
      context: .
    ports:
      - "5000:5000"
    volumes:
      - ./exchange.py:/opt/airflow/exchange.py 
    depends_on:
      - postgres
    networks:
      - confluent
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ['CMD', 'bash', '-c', "echo 'ruok' | nc localhost 2181"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - confluent

  broker:
    image: confluentinc/cp-server:7.4.0
    hostname: broker
    container_name: broker
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://schema-registry:8083
      CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: broker:29092
      CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      CONFLUENT_METRICS_ENABLE: 'false'
      CONFLUENT_SUPPORT_CUSTOMER_ID: 'anonymous'
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "bash", "-c", 'nc -z broker 29092' ]
      interval: 10s
      timeout: 5s
      retries: 5
      
  schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8083:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8081/" ]
      interval: 30s
      timeout: 10s
      retries: 5

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.4.0
    hostname: control-center
    container_name: control-center
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:29092'
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8083"
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      CONFLUENT_METRICS_ENABLE: 'false'
      PORT: 9021
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9021/health" ]
      interval: 30s
      timeout: 10s
      retries: 5

  airflow-webserver:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__WEBSERVER__WEB_SERVER_PORT: 8080
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init &&
           airflow users create 
           --username airflow 
           --password airflow 
           --firstname Air 
           --lastname Flow 
           --role Admin 
           --email airflow@example.com &&
           airflow webserver"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./outputs:/opt/airflow/outputs
    networks:
      - confluent

  airflow-scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      - airflow-webserver
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    command: airflow scheduler
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./outputs:/opt/airflow/outputs
    networks:
      - confluent
  spark-master:
    image: bitnami/spark:latest
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "7077:7077"
      - "8081:8081"
    networks:
      - confluent

  spark-worker:
    image: bitnami/spark:latest
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master
    ports:
      - "8082:8082"
    networks:
      - confluent

  jupyter:
    build:
      context: .
      dockerfile: jupyter/Dockerfile
    depends_on:
      - spark-master
    ports:
      - "8888:8888"
    volumes:
      - ./jupyter/notebooks:/home/jovyan/work/notebooks
    networks:
      - confluent
  mongodb:
    image: mongo:latest
    container_name: mongodb
    ports:
      - "27017:27017"
    networks:
      - confluent
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: example

volumes:
  pgdata:
  mongo_data:

networks:
  confluent:
    driver: bridge</pre>

## 🚀 Running Docker Services ## 
Once your setup is ready, you can start all Docker containers using the following command:

<pre>docker compose up --build</pre>

This will build the images and initialize all services. Be sure to run this command at least once to bootstrap the environment correctly.


## 🧩 Project Structure ##
The project is organized in a modular way. The initial module handles the following tasks:

Retrieving live stock price data from the API
Sending this data to Kafka
Utilizing the requests library with Apache Spark Streaming for real-time data ingestion

<pre># stock_price_producer.py

from pyspark.sql import SparkSession
import json
import logging
import requests
from datetime import datetime
import pandas
from pyspark.sql.functions import *

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaStockProducer")

producer_params = {
    'kafka.bootstrap.servers': 'broker:29092',
    'topic': 'stock-market-producer'
}

url = "http://flask-api:5000/api/market"

def spark_session():
    spark = SparkSession.builder \
        .appName("KafkaProducerStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    return spark

def fetch_data():
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"API error {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None
    
def spark_dataframe(spark, data):
    try:
        df = spark.createDataFrame([(json.dumps(data),)], ['value'])
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", producer_params['kafka.bootstrap.servers']) \
            .option("topic", producer_params['topic']) \
            .save()
        logger.info("Data sent to Kafka.")
        return df
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")

def process_stream(df, epoch_id):
    spark = spark_session()
    data = fetch_data()
    if not data:
        logger.warning("No data to process.")
        return
    logger.info(f"company name {data[0]['name']}")

    spark_dataframe(spark, data)

def main():
    logger.info("Starting Kafka producer streaming...")

    spark = spark_session()

    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load()

    query = trigger_df.writeStream \
        .foreachBatch(process_stream) \
        .option("checkpointLocation", "/tmp/checkpoint-stock-market-producer") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()</pre>

With the data now being published to Kafka, the next phase involves consuming it through Spark Streaming and storing it in MongoDB. We take advantage of MongoDB’s excellent write performance and scalability to manage the continuous flow of stock data effectively. The following module is responsible for this part of the streaming pipeline.

<pre># stock_price_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaStockLogger")

def create_spark_session():
    logger.info("Creating Spark session...")
    try:
        spark = SparkSession.builder \
            .appName("KafkaStockLogger") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error("Failed to create Spark session.")
        logger.error(e)
        raise

def get_stock_schema():
    logger.info("Defining schema for stock data...")
    try:
        schema = StructType([
            StructField("current_price", DoubleType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("percent_change", DoubleType(), True),
            StructField("symbol", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("volume", DoubleType(), True)
        ])
        logger.info("Schema defined successfully.")
        return schema
    except Exception as e:
        logger.error("Failed to define schema.")
        logger.error(e)
        raise

def consume_data(spark, schema):
    logger.info("Starting to consume data from Kafka...")
    try:
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "stock-market-producer") \
            .option("startingOffsets", "earliest") \
            .load()

        parsed_array_df = raw_df.select(from_json(col("value").cast("string"), ArrayType(schema)).alias("data"))
        exploded_df = parsed_array_df.select(explode(col("data")).alias("stock"))
        df = exploded_df.select("stock.*")

        logger.info("Kafka stream configured successfully.")
        return df
    except Exception as e:
        logger.error("Failed to consume data from Kafka.")
        logger.error(e)
        raise

def process_batch(df, epoch_id):
    if df.isEmpty():
        logger.info(f"[Epoch {epoch_id}] No data.")
        return

    logger.info(f"[Epoch {epoch_id}] Processing {df.count()} rows.")

    try:
        # Convert to Pandas and then to MongoDB
        records = df.toPandas().to_dict("records")
        
        if records:
            client = MongoClient("mongodb://root:example@mongodb:27017")
            db = client["mydb"]
            collection = db["mycollection"]
            collection.insert_many(records)
            client.close()
            logger.info(f"[Epoch {epoch_id}] Successfully written to MongoDB.")
    except Exception as e:
        logger.error(f"[Epoch {epoch_id}] MongoDB write failed: {e}")

def stream_main():
    logger.info("Initializing stream processing...")
    try:
        spark = create_spark_session()
        schema = get_stock_schema()
        df = consume_data(spark, schema)

        query = df.writeStream \
                .foreachBatch(process_batch) \
                .outputMode("append") \
                .start()

        logger.info("Streaming query started. Awaiting termination...")
        query.awaitTermination()
    except Exception as e:
        logger.error("Stream processing failed.")
        logger.error(e)

if __name__ == "__main__":
    stream_main()</pre>

## Inserting Data into PostgreSQL ##
At this stage, we extract the processed data from MongoDB and insert it into PostgreSQL. Since real-time stock updates aren’t necessary in our relational database, we focus only on storing key information about stocks.

This task is performed once daily, which makes PostgreSQL a suitable choice for archival and reporting purposes. To automate the ETL workflow, we use Apache Airflow.

The script from from_mongo_to_postgres.py is responsible for pulling the data from MongoDB and loading it into PostgreSQL.

<pre># from_mongo_to_postgres.py

from pymongo import MongoClient
import psycopg2
import logging
from datetime import datetime, time as dtime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaStockConsumer")

def mongo_connection():
    try:
        logger.info("Connecting to MongoDB..")
        client = MongoClient("mongodb://root:example@mongodb:27017")
        db = client["mydb"]
        collection = db["mycollection"]
        logger.info("Connected to MongoDB successfully.")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def postgres_connection():
    try:
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432")
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL successfully.")
        return cursor, conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def get_data_from_mongo(cursor, collection):
    try:
        logger.info("Fetching data from MongoDB collection...")
        documents = collection.find({})
        first5 = collection.find().limit(1)
        for doc in first5:
            logger.info(doc)
        for doc in documents:
            symbol = doc["symbol"]
            timestamp_str = doc.get("timestamp")
            if not timestamp_str:
                logger.warning(f"Missing timestamp in document with symbol {doc.get('symbol')}")
                continue 
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            date = dt.date()
            cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
            result = cursor.fetchone()
            if not result:
                logger.warning(f"Company symbol {symbol} not found in companies table.")
                continue
            company_id = result[0]

            open_price = doc.get("open", 0.0)
            time_ = dt.time()
            current_price = doc.get("current_price", 0.0)

            if time_ == dtime(18, 0):
                close_price = current_price

                start_dt = datetime.combine(date, dtime(8, 0))
                end_dt = datetime.combine(date, dtime(18, 0, 0))

                high_query = {
                    "symbol": symbol,
                    "timestamp": {
                        "$gte": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "$lte": end_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
                all_prices = list(collection.find(high_query))

                if not all_prices:
                    logger.warning(f"No prices found for symbol {symbol} on {date}")
                    continue

                high_price = max(p.get("current_price", 0.0) for p in all_prices)
                low_price = min(p.get("current_price", 0.0) for p in all_prices)
                total_volume = max(int(p.get("volume", 0)) for p in all_prices)

                cursor.execute("""
                    INSERT INTO prices (company_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, date) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (company_id, date, open_price, high_price, low_price, close_price, total_volume))

    except Exception as e:
        logger.error(f"Error while processing data: {e}")
        raise

def from_mongo_to_postgres():
    collection = mongo_connection()
    cursor, conn = postgres_connection()
    try:
        get_data_from_mongo(cursor, collection)
        conn.commit()
        logger.info("Data inserted/updated successfully.")
    except Exception as e:
        logger.error(f"Failed during data transfer: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    from_mongo_to_postgres()</pre>


## Reporting ## 

To support reporting, we utilize a script integrated within the Airflow workflow. This script executes SQL queries to extract stock data and exports the results as CSV files. These files are saved in an outputs directory for convenient access and further analysis.

Automating this task with Airflow ensures the daily generation of reports, making the entire data pipeline — from ingestion to reporting — more efficient and reliable.

<pre># sql_report.py

import os 
import psycopg2
import logging
from datetime import date
import pandas as pd

report_date = date.today()

output_dir = "/opt/airflow/outputs"
os.makedirs(output_dir, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("DailyReportGenerator")

def postgres_connection():
    try:
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()
        logger.info("PostgreSQL connection successful.")
        return cursor, conn
    except Exception as e:
        logger.error("PostgreSQL connection failed.")
        logger.error(e)
        raise


def report_top_volume(report_date, cursor):
    logger.info("Starting top volume report...")
    try:
        query = """
            SELECT c.name, c.symbol, p.volume 
            FROM companies c
            JOIN prices p ON c.id = p.company_id
            WHERE p.date = %s
            ORDER BY p.volume DESC
            LIMIT 5;
        """
        cursor.execute(query, (report_date,))
        rows = cursor.fetchall()
        for row in rows:
            logger.info(f"Company: {row[0]}, Symbol: {row[1]}, Volume: {row[2]}")
        logger.info("Top volume report completed successfully.")
    except Exception as e:
        logger.error("Top volume report failed.")
        logger.error(e)


def report_top_gainers(report_date, cursor):
    logger.info("Starting top gainers report...")
    try:
        query = """
            SELECT c.name, c.symbol,
                   ROUND(((p.close - p.open) / NULLIF(p.open, 0)) * 100, 2) AS pct_change
            FROM companies c
            JOIN prices p ON c.id = p.company_id
            WHERE p.date = %s
            ORDER BY pct_change DESC
            LIMIT 5;
        """
        cursor.execute(query, (report_date,))
        rows = cursor.fetchall()
        for row in rows:
            logger.info(f"Company: {row[0]}, Symbol: {row[1]}, Change: {row[2]}%")
        logger.info("Top gainers report completed successfully.")
    except Exception as e:
        logger.error("Top gainers report failed.")
        logger.error(e)


def report_avg_close_by_sector(report_date, cursor):
    logger.info("Starting average close by sector report...")
    try:
        query = """
            SELECT s.name AS sector, ROUND(AVG(p.close), 2) AS avg_close
            FROM prices p
            JOIN companies c ON c.id = p.company_id
            JOIN sectors s ON c.sector_id = s.id
            WHERE p.date = %s
            GROUP BY s.name
            ORDER BY avg_close DESC;
        """
        cursor.execute(query, (report_date,))
        rows = cursor.fetchall()
        for row in rows:
            logger.info(f"Sector: {row[0]}, Avg Close Price: {row[1]}")
        logger.info("Average close by sector report completed successfully.")
    except Exception as e:
        logger.error("Average close by sector report failed.")
        logger.error(e)


def report_total_volume_by_exchange(report_date, cursor):
    logger.info("Starting total volume by exchange report...")
    try:
        query = """
            SELECT e.name AS exchange, SUM(p.volume) AS total_volume
            FROM prices p
            JOIN companies c ON c.id = p.company_id
            JOIN exchanges e ON c.exchange_id = e.id
            WHERE p.date = %s
            GROUP BY e.name
            ORDER BY total_volume DESC;
        """
        cursor.execute(query, (report_date,))
        rows = cursor.fetchall()
        for row in rows:
            logger.info(f"Exchange: {row[0]}, Total Volume: {row[1]}")
        logger.info("Total volume by exchange report completed successfully.")
    except Exception as e:
        logger.error("Total volume by exchange report failed.")
        logger.error(e)


def report_top_losers(report_date, cursor):
    logger.info("Starting top losers report...")
    try:
        query = """
            SELECT c.name, c.symbol,
                   ROUND(((p.close - p.open) / NULLIF(p.open, 0)) * 100, 2) AS pct_change
            FROM companies c
            JOIN prices p ON c.id = p.company_id
            WHERE p.date = %s AND p.open > 0 AND p.close < p.open
            ORDER BY pct_change ASC
            LIMIT 5;
        """
        cursor.execute(query, (report_date,))
        rows = cursor.fetchall()
        for row in rows:
            logger.info(f"Company: {row[0]}, Symbol: {row[1]}, Change: {row[2]}%")
        logger.info("Top losers report completed successfully.")
    except Exception as e:
        logger.error("Top losers report failed.")
        logger.error(e)
        
def write_to_csv(filename, data):
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"Data written to {filename} successfully using pandas.")
    except Exception as e:
        logger.error(f"Failed to write data to {filename}: {e}")


def generate_daily_report():
    cursor, conn = postgres_connection()
    try:
        top_volume_companies  = report_top_volume(report_date, cursor)
        write_to_csv(os.path.join(output_dir, f"top_volume_companies{report_date}.csv"), top_volume_companies)
        
        top_gainer_companies = report_top_gainers(report_date, cursor)
        write_to_csv(os.path.join(output_dir, f"top_gainer_companies{report_date}.csv"), top_gainer_companies)
        
        avg_close_by_sector = report_avg_close_by_sector(report_date, cursor)
        write_to_csv(os.path.join(output_dir, f"avg_close_by_sector{report_date}.csv"), avg_close_by_sector)

        total_volume_by_exchange = report_total_volume_by_exchange(report_date, cursor)
        write_to_csv(os.path.join(output_dir, f"total_volume_by_exchange{report_date}.csv"), total_volume_by_exchange)
        
        top_loser_companies = report_top_losers(report_date, cursor)
        write_to_csv(os.path.join(output_dir, f"top_loser_companies{report_date}.csv"), top_loser_companies)
        
        logger.info("All reports completed.")
    finally:
        conn.close()
        logger.info("PostgreSQL connection closed.")


if __name__ == "__main__":
    generate_daily_report()</pre>

## Triggering the Airflow Pipeline with the DAG File ## 

To execute the full pipeline, we use an Airflow Directed Acyclic Graph (DAG) file. This DAG manages the workflow by coordinating the sequence of tasks, making sure data is retrieved, processed, and loaded into PostgreSQL at the right times.

Scheduled to run once daily, the DAG performs the insertion of the important stock data into PostgreSQL and also oversees the generation of daily CSV reports through SQL queries.

Below is the dag.py file, which defines the tasks and their execution order:

<pre># dag

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../opt/airflow/src')))

from from_mongo_to_postgres import from_mongo_to_postgres
from sql_report import generate_daily_report

logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025,4,14),
    "catchup": False,
}

dag = DAG(
    dag_id="stocks_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    max_active_runs=1
)

def start_job():
    logging.info("Starting the pipeline.")

def fetch_data_from_mongo():
    try:
        logger.info("Fetching data from MongoDB")
        from_mongo_to_postgres()
        logger.info("The data has been inserted into Postgresql")

    except Exception as e:
        logger.error(f"An error occured while fetching data {e}")

def daily_report():
    generate_daily_report()
    logger.info("The daily report prepared and saved into CSV files")

def end_job():
    logger.info("All process completed.")


start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

fetching_data_task = PythonOperator(
    task_id='fetch_data_job',
    python_callable=fetch_data_from_mongo,
    dag=dag
)

daily_report_task = PythonOperator(
    task_id='daily_report_job',
    python_callable= daily_report,
    dag = dag
)

end_task = PythonOperator(
    task_id= 'end_job',
    python_callable=end_job,
    dag=dag
)

start_task >> fetching_data_task >> daily_report_task >> end_task</pre>

As we can see below, the DAG has executed successfully. In case of any potential errors, the logs should be reviewed for detailed insights.

![0_CP7zZU4XrmIIXyZO](https://github.com/user-attachments/assets/1b7d5963-f4f5-4648-b004-6132a1eea60d)



