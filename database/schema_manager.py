from pathlib import Path
from mysql.connector import Error
SQL_FILE_PATH = Path("../sql/schema.sql")
def create_mysql_schema(connection,cursor):
    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    connection.commit()
    print(f"--- create {database} success --------")
    connection.database = database

    try:
        with open(SQL_FILE_PATH,"r") as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            # print(sql_commands)
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"---------------- Executed mysql commands --------------")
            connection.commit()
    except Error as e:
        connection.rollback()
        raise Exception(f"---------- Fail to create Mysql schema: {e} ----------") from e

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    # print(cursor.fetchall())
    # cursor.fetchall()
    #  validate table if exists
    tables = [row[0] for row in cursor.fetchall()]
    print(tables)
    if "users" not in tables or "repositories" not in tables:
       raise ValueError(f"------ table does not exists ---------------")

    cursor.execute("select * from users where user_id = 1;")

    # validate data when insert
    user = cursor.fetchone()
    if not user:
        raise ValueError("------------- User not found --------------------")
    print("---------------- validate schema in mysql success -------------")

def create_mongodb_schema(db):
    db[1].drop_collection("users")
    db[1].create_collection("users",validator = {
        "$jsonSchema":{
            "bsonType": "object",
            "required": ["user_id","login"],
            "properties":{
                "user_id":{
                    "bsonType" : "int"
                },
                "login": {
                  "bsonType": ["string","null"]
                },
                "gravatar_id": {
                    "bsonType": ["string","null"]
                },
                "url": {
                    "bsonType" : ["string","null"]
                },
                "avatar_url": {
                    "bsonType": ["string","null"]
                }
            }
        }
    })

    print("----------------- created collection users in mongodb -----------------")

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(f"----------- collection: {collections}")

    if "users" not in collections:
        raise ValueError("-------------collections in Mongodb doesn't exist--------------")

    user = db.users.find_one({"user_id": 1})

    if not user:
        raise ValueError("---------- user_id not found in mongodb --------------")

    print("-------------validate in MongoDB success---------------")