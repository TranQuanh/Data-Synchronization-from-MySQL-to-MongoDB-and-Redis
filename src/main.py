from database.mysql_connect import MySQLConnect
from config.database_config import get_database_config
from database.redis_connect import RedisConnect
from database.schema_manager import create_mysql_schema,validate_mysql_schema, create_mongodb_schema, validate_mongodb_schema
from database.mongodb_connect import  MongoDBConnect
from pathlib import Path
SAMPLE_PATH =  Path('../data/sample.json')
def main(config):

    # ============= MYSQL =================
    with  MySQLConnect(config["mysql"].host,config["mysql"].port,config["mysql"].user,config["mysql"].password) as mysql_client:
        connection, cursor = mysql_client.connect()
        create_mysql_schema(connection,cursor)
        cursor.execute("INSERT INTO users(user_id, login, gravatar_id, url, avatar_url) VALUES(%s,%s,%s,%s,%s)",(1,"test","","https://test.com","https://avatar.com"))
        connection.commit()
        print("----------- inserted data to mysql -----------------")
        validate_mysql_schema(cursor)

    # ================= MongoDB =================
    with MongoDBConnect(config["mongodb"].uri,config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        mongo_client.db.users.insert_one({
            "user_id": 1,
            "login":"dangdat",
            "gravatar_id": "test gravatar_id",
            "url": "https://testurl.url",
            "avatar_url": "https: //test_url.avatar_url"
        })
        print("-------------------- inserted 1 record to MongoDB --------------------------")
        validate_mongodb_schema(mongo_client.db)

    # ================= Redis  ========================
    print(config["redis"])
    with RedisConnect(config["redis"].host,config["redis"].port,config["redis"].password,config["redis"].database) as redis_client:
        redis_client.write_data(SAMPLE_PATH)
        redis_client.get_data(1843574)
if __name__ == "__main__":
    config = get_database_config()
    main(config)

