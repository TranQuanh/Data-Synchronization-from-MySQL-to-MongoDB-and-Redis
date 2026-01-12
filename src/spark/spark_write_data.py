from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import json
from database.redis_connect import RedisConnect
from database.mongodb_connect import  MongoDBConnect
from pyspark.sql.functions import col

from database.mysql_connect import MySQLConnect


class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self,df_write:DataFrame,table_name: str,jdbc_url: str, config:Dict,mode: str="append"):
        # python cursor add column spark_term
        try:
            with  MySQLConnect(config["host"], config["port"], config["user"],
                               config["password"]) as mysql_client:
                connection, cursor = mysql_client.connect()

                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                print(f"-----add column spark_temp to mysql -----")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------- fail to connect Mysql: {e}--------")
        #  spark write dataframe to mysql
        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        print(f"------------spark write data to mysql table: {table_name} success--------------")

    def validate_spark_mysql(self,df_write: DataFrame,table_name: str,jdbc_url: str, config:Dict, mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp  = 'sparkwrite') AS subq" )\
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        # df_read.show()

        def substract_dataframe(df_write: DataFrame,df_read: DataFrame):
            result = df_write.exceptAll(df_read)
            print(f"-------Records missing: {result.count()}----------------")
            result.show()

            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()
                print(f"------------spark write data missing to mysql table: {table_name} success with :{result.count()} records--------------")


        # check count of records
        if df_write.count() == df_read.count():
            print(f"------------ validate {df_read.count()} records success--------------")
            substract_dataframe(df_write,df_read)
        else:
            print(f"-----------spark insert missing records in mysql---------")
            substract_dataframe(df_write,df_read)

    #   drop column spark_temp
        try:
            with  MySQLConnect(config["host"], config["port"], config["user"],
                               config["password"]) as mysql_client:
                connection, cursor = mysql_client.connect()

                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp ")
                connection.commit()
                print(f"-----DROP column spark_temp to mysql -----")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------- fail to connect Mysql: {e}--------")

        print(f"------------Validate spark write data to MySQL success-------------")

    def spark_write_mysql_primaryKey(self,df_write:DataFrame,jdbc_url: str, config:Dict,mode: str="append"):
        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "spark_table_temp") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        print(f"------------spark write data to mysql table: spark_table_temp success--------------")

    def validate_spark_write_primaryKey(self,df_write: DataFrame,jdbc_url: str, config:Dict, mode: str = "append"):
        try:
            df_read = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "spark_table_temp") \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            df_temp = df_write.exceptAll(df_read)
            if df_temp.count() != 0:
                df_temp.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "spark_table_temp") \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()
                print(
                    f"------------spark write data missing to mysql table: spark_table_temp success with :{df_temp.count()} records--------------")

        except Exception as e:
            raise Exception(f"---fail to write missing record to spark_table_temp in mysql")

    def insert_data_mysql_primaryKey(self,config: Dict):
        try:
            with  MySQLConnect(config["host"], config["port"], config["user"],
                               config["password"]) as mysql_client:
                connection, cursor = mysql_client.connect()
                database = "github_data"
                connection.database = database

                cursor.execute(
                    "select a.* from spark_table_temp a left join users b on a.user_id = b.user_id where b.user_id is null;"
                )
                records = []
                row = cursor.fetchone()

                while row:
                    records.append(row)
                    row = cursor.fetchone()

                for rec in records:
                    try:
                        cursor.execute(
                            "INSERT INTO users(user_id,login, gravatar_id, url, avatar_url) values(%s,%s,%s,%s, %s)"
                            , rec
                        )
                        connection.commit()
                        print("----insert data into mysql successfully-----")
                    except Exception as e:
                        print(f"Erro inserting record {rec}: {str(e)}")
                        continue
                connection.commit()
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------- fail to connect Mysql: {e}--------")
    def spark_write_mongodb(self, df : DataFrame, uri: str, database: str, collection: str, mode: str="append"):
        df.write \
            .format("mongodb") \
            .option("spark.mongodb.write.connection.uri", uri) \
            .option("spark.mongodb.write.database", database) \
            .option("spark.mongodb.write.collection", collection) \
            .mode(mode) \
            .save()


        print(f"------------spark write data to mongodb collection: {collection} success-----------")

    def validate_spark_mongodb(self,df_write: DataFrame,uri: str, database: str,collection: str,mode : str= "append"):
        query = {"spark_temp": "sparkwrite"}
        df_read = self.spark.read \
            .format("mongodb") \
            .option("spark.mongodb.read.connection.uri", uri) \
            .option("spark.mongodb.read.database", database) \
            .option("spark.mongodb.read.collection", collection) \
            .option("aggregation.pipeline", str([{"$match": query}])) \
            .load()
        # df_read.show()
        df_filtered = df_read.select("user_id","login","gravatar_id","url","avatar_url","spark_temp")
        # df_filtered.show()
        # print(df_filtered.count())
        # print(df_read.count())
        df_temp = df_write.exceptAll(df_filtered)

        if df_temp.count() !=0:
            df_temp.write \
                .format("mongodb") \
                .option("spark.mongodb.write.connection.uri", uri) \
                .option("spark.mongodb.write.database", database) \
                .option("spark.mongodb.write.collection", collection) \
                .mode(mode) \
                .save()
            print(f"----Spark wrote missing records to MongoDb collection {collection} : {df_temp.count()} records-------")

        try:
            with MongoDBConnect(uri,database) as mongo_client:
                db=mongo_client.db
                result = db[collection].update_many(
                    {},
                    {"$unset":{"spark_temp":""}}
                )
                print(f"----------Drop field spark temp in MongoDb Collection {collection}--------")
        except Exception as e:
            raise Exception(f"-----------Fail to read from or write to MongoDB: {str(e)}------------")
    def spark_write_redis(self, df : DataFrame,table: str,key_column: str,config: dict, mode: str="append"):
        df.write \
            .format("org.apache.spark.sql.redis") \
            .option("host", config["host"]) \
            .option("port", config["port"]) \
            .option("auth", config["password"]) \
            .option("dbNum", config["database"]) \
            .option("table", table) \
            .option("key.column", key_column) \
            .mode("append") \
            .save()

        print(f"------------spark write data to Redis database: {config["database"]} success-----------")
    def validate_spark_redis(self, df_write: DataFrame, table: str, key_column: str, config: dict):
        df_read = self.spark.read \
            .format("org.apache.spark.sql.redis") \
            .option("host", config["host"]) \
            .option("port", config["port"]) \
            .option("auth", config["password"]) \
            .option("dbNum", config["database"]) \
            .option("table", table) \
            .option("key.column", key_column) \
            .load()

        df_filtered = df_read.filter(col("spark_temp") == "sparkwrite")

        df_temp = df_write.exceptAll(df_filtered)

        if df_temp.count() != 0:
            df_temp.write \
                .format("org.apache.spark.sql.redis") \
                .option("host", config["host"]) \
                .option("port", config["port"]) \
                .option("auth", config["password"]) \
                .option("dbNum", config["database"]) \
                .option("table", table) \
                .option("key.column", key_column) \
                .mode("append") \
                .save()
            print(
                f"----Spark wrote missing records to redis database {config["database"]} : {df_temp.count()} records-------")

    #     remove spark_temp column
        try:
            with RedisConnect(config["host"],config["port"],config["password"],config["database"]) as redis_client:
                redis_client.delete_field_all_keys("users","spark_temp")
            print(f"----------Drop field spark temp in redis database {config["database"]}--------")
        except Exception as e:
            raise Exception(f"-----------Fail to read from or write to Redis: {str(e)}------------")
    def write_all_database(self, df: DataFrame, mode: str = "append"):
        self.spark_write_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )
        self.spark_write_mongodb(
            df,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
            mode
        )

        self.spark_write_redis(
            df,
            "users",
            "user_id",
            self.db_config["redis"],
            mode
            )
        print(f"----------------spark write all database(mysql, mongodb, redis) !!!!!!!!!!!!----------")
    def validate_spark_all_database(self,df_write: DataFrame, mode: str= "append"):
        self.validate_spark_mysql(
            df_write,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )

        self.validate_spark_mongodb(
            df_write,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"]
        )
        self.validate_spark_redis(
            df_write,
            "users",
            "user_id",
            self.db_config["redis"]
        )

        print(f"---------------------validate all database success with spark---------------------")
