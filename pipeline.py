from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configs import kafka_config
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = SparkSession.builder \
    .appName("KafkaAlertPipeline") \
    .master("local[*]") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("subscribe", "hanna_dunska_input_topic") \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select(
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("timestamp"),
        col("data.temperature"),
        col("data.humidity")
    )

agg_df = parsed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        min("timestamp").alias("timestamp")
    )

alerts_schema = StructType([
    StructField("id", IntegerType()),
    StructField("humidity_min", IntegerType()),
    StructField("humidity_max", IntegerType()),
    StructField("temperature_min", IntegerType()),
    StructField("temperature_max", IntegerType()),
    StructField("code", IntegerType()),
    StructField("message", StringType())
])

alerts_df = spark.read \
    .option("header", "true") \
    .schema(alerts_schema) \
    .csv("alerts_conditions.csv")

joined = agg_df.crossJoin(alerts_df).filter(
    ((col("avg_humidity") >= col("humidity_min")) | (col("humidity_min") == -999)) &
    ((col("avg_humidity") <= col("humidity_max")) | (col("humidity_max") == -999)) &
    ((col("avg_temperature") >= col("temperature_min")) | (col("temperature_min") == -999)) &
    ((col("avg_temperature") <= col("temperature_max")) | (col("temperature_max") == -999))
)

alerts_to_kafka = joined.selectExpr(
    "CAST(code AS STRING) AS key",
    "to_json(struct(window.start AS window_start, window.end AS window_end, avg_temperature, avg_humidity, code, message, timestamp)) AS value"
)

query = alerts_to_kafka.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
    .option("topic", "hanna_dunska_output_topic") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("checkpointLocation", "/tmp/checkpoints_alerts") \
    .start()

query.awaitTermination()
