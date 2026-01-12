import redis
import json

from redis.connection import ConnectionError

class RedisConnect:
    def __init__(self, host:str, port: int, password: str, database: int):
        self.host = host
        self.port = port
        self.password = password
        self.database = database
        # self.username = user
        self.client = None

    def connect(self):
        try:
            self.client = redis.StrictRedis(
                host= self.host,
                port= int(self.port),
                password=self.password,
                username = None,
                db= self.database,
                decode_responses=True)
        except ConnectionError as e:
            raise Exception(f"---------------Failed to Connect Redis: {e}-------------")
        return self.client

    def disconnect(self):
        self.client.close()

    def __enter__(self):
        self.connect()
        # Thử ping để kiểm tra mật khẩu ngay lập tức
        try:
            self.client.ping()
            print("--- Redis Connected Successfully ---")
        except redis.exceptions.AuthenticationError:
            print("--- LỖI: Redis từ chối vì có Username 'default'. Hãy đặt username=None trong code! ---")
            raise
        return self # Trả về self để gọi được hàm write_data trong main.py

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.disconnect()

    def write_data(self, json_path: str|None):
        if json_path:
            with open(json_path, 'r') as file:
                data = json.load(file)
                # print(data)
        if data["actor"]:
            self.client.hset("1843574", mapping=data["actor"])
            print("------------------Data Writen - Key: 1843574 ----------------------")
        else:
            print("---------------------Error: Cannot find data-------------------")

    def get_data(self, key_):
        data = self.client.hgetall(f"{key_}")
        print(data)
        return data

    def delete_field_all_keys(self, table_prefix: str, field_to_delete: str):
        """
        Xóa một trường cụ thể (ví dụ: spark_temp) khỏi tất cả các bản ghi có prefix
        table_prefix: ví dụ 'users:'
        field_to_delete: ví dụ 'spark_temp'
        """
        try:
            count = 0
            # Sử dụng scan_iter để duyệt qua các key mà không làm treo Redis
            for key in self.client.scan_iter(match=f"{table_prefix}*"):
                # Lệnh hdel trả về 1 nếu xóa thành công, 0 nếu field không tồn tại
                result = self.client.hdel(key, field_to_delete)
                if result:
                    count += 1

            print(f"--- Đã xóa trường '{field_to_delete}' khỏi {count} bản ghi ---")
        except Exception as e:
            print(f"--- Lỗi khi xóa field: {e} ---")