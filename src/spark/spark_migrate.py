from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from config.spark_config import SparkConnect
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, BooleanType
from config.database_config import get_spark_config
from src.spark.spark_write_data import SparkWriteDatabase
from migration.kafka_connect_cdc import KafkaConnectMysql
from migration.MongoSink import MongoSink
from config.database_config import get_database_config
def main():
    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        "com.redislabs:spark-redis_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ]

    #  setup Spark Session
    spark_connect = SparkConnect(
        app_name="dat_dz",
        master_url= "local[*]",
        executor_memory= "2g",
        executor_cores= 1,
        driver_memory= "2g",
        num_executors= 1,
        jar_packages= jars,
        log_level= "INFO"
    )

    SCHEMA = StructType([
        StructField("schema", StructType([
            StructField("type", StringType(), True),
            StructField("fields", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("fields", ArrayType(StructType([
                    StructField("type", StringType(), True),
                    StructField("optional", BooleanType(), False),
                    StructField("field", StringType(), True),
                ]), True)),
                StructField("name", StringType(), True),
                StructField("field", StringType(), True)
            ]), True), True)
        ]), True),

        StructField("payload", StructType([
            StructField("before", StructType([
                StructField("user_id", IntegerType(), True),
                StructField("login", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("avatar_url", StringType(), True)
            ]), True),
            StructField("after", StructType([
                StructField("user_id", IntegerType(), True),
                StructField("login", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("avatar_url", StringType(), True)
            ]), True),
            StructField("op", StringType(),False)
        ]), True)
    ])

    kafka_reader = KafkaConnectMysql(spark_connect.spark,SCHEMA)
    mongo_sink = MongoSink(config["mongodb"].db_name,config["mongodb"].collection, config["mongodb"].uri)
    changes_df = kafka_reader.raw_msg

    KafkaConnectMysql.start_streaming(
        message=changes_df,
        func=mongo_sink.save_batch,
        mode="append"
    )
if __name__ == "__main__":
    config = get_database_config()
    main()