from bson import Int64
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from database.mongodb_connect import  MongoDBConnect
class MongoSink:
    def __init__(self, db_name, collection_name,uri):
        self.db_name = db_name
        self.collection_name = collection_name
        self.uri = uri
    def save_batch(self, batch_df, batch_id):
        batch_df.persist()
        print(f"--- process  Batch {batch_id} into MongoDB ---")
        if not batch_df.isEmpty():
            upserts = batch_df.filter(col("data_change") != "delete") \
                .select(col("data_after.user_id").alias("user_id"),
                        col("data_after.login").alias("login"),
                        col("data_after.gravatar_id").alias("gravatar_id"),
                        col("data_after.url").alias("url"),
                        col("data_after.avatar_url").alias("avatar_url"))

            deletes = batch_df.filter(col("data_change") == "delete") \
                .select(col("data_before.user_id").alias("user_id"),
                        col("data_before.login").alias("login"),
                        col("data_before.gravatar_id").alias("gravatar_id"),
                        col("data_before.url").alias("url"),
                        col("data_before.avatar_url").alias("avatar_url"))

            if not deletes.isEmpty():
                ids = [user_id[0] for user_id in deletes.select("users_id").collect()]
                with MongoDBConnect(self.uri, self.db_name,self.collection_name) as mongo_client:
                    result = mongo_client.connect()[self.collection_name].delete_many({'user_id': {'$in': ids}})
                    print(f"--------------- delete user id {ids} in MongoDB ----------------------")

            if not upserts.isEmpty():
                upserts.write \
                    .format("mongodb") \
                    .option("spark.mongodb.write.connection.uri", self.uri) \
                    .option("spark.mongodb.write.database", self.db_name) \
                    .option("spark.mongodb.write.collection", self.collection_name) \
                    .mode("append") \
                    .save()
            print("--------------------- Updated/Inserted Record(s) Successfully ----------------------")
            batch_df.unpersist()