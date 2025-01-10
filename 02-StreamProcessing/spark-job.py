import os
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


# Configs for Hadoop in Windows
HADOOP_HOME = os.getenv("HADOOP_HOME")
if (not HADOOP_HOME) and (os.name == "nt"):
    os.environ["HADOOP_HOME"] = r"C:\winutils-master\hadoop-3.3.6"
    HADOOP_HOME = os.getenv("HADOOP_HOME")
    os.environ["PATH"] = fr"{os.environ["PATH"]};{HADOOP_HOME}\bin"
    print("Hadoop Native Libraries configured!")


# Configs for Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "avroic")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "spark-aggregator")
DEPLOY_MODE = os.getenv("DEPLOY_MODE", "client")

# Configs for Spark APP
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", 2))


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("InteractionsAggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .config("spark.submit.deployMode", DEPLOY_MODE) \
    .getOrCreate()
print("Spark Session created")


# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "avroic") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka messages Schema
schema = StructType([
    StructField('user_id', StringType()),
    StructField('item_id', StringType()),
    StructField('interaction_type', StringType()),
    StructField('timestamp', TimestampType())
])

# Parse Kafka messages
interactions = df \
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*')

interactions.writeStream \
    .option("checkpointLocation", "./spark-table-checkpoint/interactions") \
    .toTable("interactions")
print("Reading from Kafka...")

###### MAIN LOOP ######
LAST_ROW_INDEX = 0
while True:
    try:
        new_index = spark.sql("select * from interactions").count()
        if new_index == LAST_ROW_INDEX:
            print(f"No new data to process! Waiting... ({LOOP_SLEEP}s)")
            sleep(LOOP_SLEEP)
            continue
    except KeyboardInterrupt:
        print("Stopping Spark Job...")
        break
###### MAIN LOOP ######
spark.stop()