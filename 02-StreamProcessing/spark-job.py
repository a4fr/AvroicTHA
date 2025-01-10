import os
import shutil
import logging
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configs for Hadoop in Windows
HADOOP_HOME = os.getenv("HADOOP_HOME")
if (not HADOOP_HOME) and (os.name == "nt"):
    os.environ["HADOOP_HOME"] = r"C:\winutils-master\hadoop-3.3.6"
    HADOOP_HOME = os.getenv("HADOOP_HOME")
    os.environ["PATH"] = fr"{os.environ['PATH']};{HADOOP_HOME}\bin"
    logging.info("Hadoop Native Libraries configured!")


# Configs for Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "avroic")

# Configs for Spark APP
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", 2))
DEPLOY_MODE = os.getenv("DEPLOY_MODE", "client")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")

# Initialize SparkSession
logging.info("Creating Spark Session...")
logging.info(f"DEPLOY_MODE: {DEPLOY_MODE}")

if DEPLOY_MODE == "client":
    spark = SparkSession.builder \
        .appName("InteractionsAggregator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.submit.deployMode", DEPLOY_MODE) \
        .getOrCreate()
    logging.info("Spark Session created")
elif DEPLOY_MODE == "cluster":
    spark = SparkSession.builder \
        .appName("InteractionsAggregator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master(SPARK_MASTER_URL) \
        .getOrCreate()
    logging.info("Spark Session created")
else:
    logging.error("Invalid DEPLOY_MODE! Use 'client' or 'cluster'")
    exit(1)


# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "avroic") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka messages Schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("item_id", StringType()),
    StructField("interaction_type", StringType()),
    StructField("timestamp", TimestampType())
])

# Remove old Checkpoint, Table files
shutil.rmtree("./spark-table-checkpoint", ignore_errors=True)
shutil.rmtree("./spark-warehouse", ignore_errors=True)

# Parse Kafka messages
interactions = df \
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*')

interactions.writeStream \
    .option("checkpointLocation", "./spark-table-checkpoint/interactions") \
    .toTable("interactions")
logging.info("Reading from Kafka...")

def agg_interactions_user():
    global spark
    sql_query = """
        with agg_interactions_user_interaction_type as (
            select user_id,
                interaction_type,
                count(*) as total_interactions,
                max(timestamp) as last_interaction
            from interactions
            group by user_id, interaction_type
            order by user_id, interaction_type
        )
        select user_id,
            sum(case when interaction_type='click' then total_interactions else 0 end) as total_click,
            sum(case when interaction_type='like' then total_interactions else 0 end) as total_like,
            sum(case when interaction_type='view' then total_interactions else 0 end) as total_view,
            sum(case when interaction_type='purchase' then total_interactions else 0 end) as total_purchase,
            round(avg(total_interactions), 2) as avg_interactions,
            sum(total_interactions) as total_interactions,
            max(last_interaction) as last_interaction
        from agg_interactions_user_interaction_type
        group by user_id
        order by total_interactions desc
    """

    result = spark.sql(sql_query)
    result.createOrReplaceTempView("agg_interactions_user")

    result.selectExpr("CONCAT('agg_interactions_user-', CAST(user_id AS STRING)) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", f"{KAFKA_TOPIC}_aggregated") \
        .save()
    logging.info("Result of (agg_interactions_user) sent to Kafka!")


def avg_interactions():
    sql_query = """
        select round(avg(total_interactions), 2) as avg_interactions,
            max(last_interaction) as last_interaction
        from agg_interactions_user
    """
    result = spark.sql(sql_query)
    result.selectExpr("'avg_interactions' AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", f"{KAFKA_TOPIC}_aggregated") \
        .save()
    logging.info("Result of (avg_interactions) sent to Kafka!")


def agg_interactions_item():
    sql_query = """
        with agg_interactions_item_interaction_type as (
            select item_id,
                interaction_type,
                count(*) as total_interactions
            from interactions
            group by item_id, interaction_type
            order by item_id, interaction_type
        )
        select item_id,
            max(total_interactions) as max_interactions,
            min(total_interactions) as min_interactions
        from agg_interactions_item_interaction_type
        group by item_id
        order by item_id
    """
    result = spark.sql(sql_query)
    result.selectExpr("CONCAT('agg_interactions_item-', CAST(item_id AS STRING)) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", f"{KAFKA_TOPIC}_aggregated") \
        .save()
    logging.info("Result of (agg_interactions_item) sent to Kafka!")




###### MAIN LOOP ######
LAST_ROW_INDEX = 0
while True:
    try:
        # Check for new data
        new_index = spark.sql("select * from interactions").count()
        if new_index == LAST_ROW_INDEX:
            logging.info(f"No new data to process! Waiting... ({LOOP_SLEEP}s)")
            sleep(LOOP_SLEEP)
            continue

        # Aggregations
        logging.info(f"Total Interactions: {new_index}")
        spark.sql("select * from interactions order by timestamp desc limit 1").show()
        LAST_ROW_INDEX = new_index

        agg_interactions_user()
        avg_interactions()
        agg_interactions_item()

    except KeyboardInterrupt:
        logging.info("Stopping Spark Job...")
        break
###### MAIN LOOP ######
spark.stop()
