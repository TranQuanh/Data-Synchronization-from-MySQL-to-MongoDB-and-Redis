import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, when, col
from pyspark.sql.types import StructType

load_dotenv()
class KafkaConnectMysql:
    def __init__(self, spark: SparkSession,schema: StructType):
        self.kafka_host = os.getenv("KAFKA_HOST")
        self.kafka_port = os.getenv("KAFKA_PORT")
        self.kafka_topic = os.getenv("KAFKA_TOPIC")
        self.spark = spark
        self.raw_msg = self.get_data(schema)
    def get_data(self, schema: StructType):
        query = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", f"{self.kafka_host}:{self.kafka_port}") \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        raw_msg = query.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("value"))

        changes = raw_msg.select(col("value.payload.before").alias("data_before"),
                                 col("value.payload.after").alias("data_after")) \
            .filter(col("data_before").isNotNull() | col("data_after").isNotNull()) \
            .withColumn("data_change", when(col("data_before").isNotNull() & col("data_after").isNull(), "delete") \
                        .when(col("data_before").isNull() & col("data_after").isNotNull(), "insert") \
                        .when(col("data_before").isNotNull() & col("data_after").isNotNull(), "update"))
        return changes

    @staticmethod
    def start_streaming(message: DataFrame, func, mode: str):
        message.writeStream \
            .foreachBatch(func) \
            .outputMode(mode) \
            .start().awaitTermination()