from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from config.spark_config import SparkConnect
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from config.database_config import get_spark_config
from src.spark.spark_write_data import SparkWriteDatabase
def main():
    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        "com.redislabs:spark-redis_2.12:3.1.0"
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

    schema = StructType([
        StructField('actor', StructType([
            StructField('id', IntegerType(), True),
            StructField('login', StringType(), True),
            StructField('gravatar_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('avatar_url', StringType(), True),
            StructField('spark_temp', StringType(), True),
        ]), True),

        StructField('repo', StructType([
            StructField('id', LongType(), False),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True),
        ]), True)
    ])
    # data = [["dat",18],
    #         ["viruss",30],
    #         ["jack97",28]]
    #
    # df = spark_connect.spark.createDataFrame(data,["name","age"])
    # df.show()

    # spark read file json and create dataframe
    df = spark_connect.spark.read.schema(schema).json("/home/quanh/PycharmProjects/DE_ETL_103/data/2015-03-01-17.json")

    # spark select col in dataframe
    df_write_table = (df.withColumn('spark_temp',lit('sparkwrite'))\
                  .select(
                    col("actor.id").alias("user_id"),
                    col("actor.login").alias("login"),
                    col("actor.gravatar_id").alias("gravatar_id"),
                    col("actor.url").alias("url"),
                    col("actor.avatar_url").alias("avatar_url"),
                    col("spark_temp")
                ))
    spark_configs = get_spark_config()
    df_write = SparkWriteDatabase(spark_connect.spark,spark_configs)
    # ==========================================
    # # mysql
    # df_write.spark_write_mysql(df_write_table, spark_configs['mysql']["table"],spark_configs['mysql']["jdbc_url"],spark_configs['mysql']["config"])
    #

    # ==========write all database =============================
    # df_write.write_all_database(df_write_table, mode= "append")
    df_write.validate_spark_all_database(df_write_table,mode="append")
    # ===========================================================
    #validate
    # df_write.validate_spark_mysql(df_write_table,spark_configs["mysql"]["table"],spark_configs["mysql"]["jdbc_url"],spark_configs["mysql"]["config"])

    # ============================================================================================
    # ====================== Cach 2: tao bang spark temp thay vi dung cot spark temp ====================
    # validate and write to database have primary key

    # write database spark_temp
    # df_write.spark_write_mysql_primaryKey(df_write_table,spark_configs["mysql"]["jdbc_url"],spark_configs["mysql"]["config"])

    #validate spark_temp
    # df_write.validate_spark_write_primaryKey(df_write_table,spark_configs["mysql"]["jdbc_url"],spark_configs["mysql"]["config"])

    # insert data from spark_temp into users
    # df_write.insert_data_mysql_primaryKey(spark_configs["mysql"]["config"])

    # ====================================
    # =======================================================================

    # #mongodb
    # df_write.spark_write_mongodb(df_write_table,spark_configs["mongodb"]["uri"],spark_configs["mongodb"]["database"],spark_configs["mongodb"]["collection"])

    # validate mongodb
    # df_write.validate_spark_mongodb(df_write_table,spark_configs["mongodb"]["uri"],spark_configs["mongodb"]["database"],spark_configs["mongodb"]["collection"])
if __name__ == "__main__":
    main()