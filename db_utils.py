import pymysql
from pymysql.err import OperationalError, ProgrammingError, IntegrityError
from datetime import datetime

class DBHelper:
    def __init__(self, host, port, user, password, db_name):
        """初始化数据库连接参数"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name,
                charset='utf8mb4'  # 支持特殊字符
            )
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            print("数据库连接成功")
        except OperationalError as e:
            raise Exception(f"数据库连接失败：{str(e)}")

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("数据库连接已关闭")

    def insert_shop_data(self, response_data, shop_detail):
        try:
            sql = """
                  INSERT INTO original_data (code, message, error_details, response_time, data, \
                                             sid, mid, name, seller_id, account_name, seller_account_id, \
                                             region, country, has_ads_setting, marketplace_id, status) \
                  VALUES (%s, %s, %s, %s, %s, \
                          %s, %s, %s, %s, %s, %s, \
                          %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                  UPDATE
                      code = \
                  VALUES (code), message = \
                  VALUES (message), error_details = \
                  VALUES (error_details), response_time = \
                  VALUES (response_time), data = \
                  VALUES (data), mid = \
                  VALUES (mid), name = \
                  VALUES (name), seller_id = \
                  VALUES (seller_id), account_name = \
                  VALUES (account_name), seller_account_id = \
                  VALUES (seller_account_id), region = \
                  VALUES (region), country = \
                  VALUES (country), has_ads_setting = \
                  VALUES (has_ads_setting), marketplace_id = \
                  VALUES (marketplace_id), status = \
                  VALUES (status) \
                  """

            # 关键修复：用 json.dumps() 序列化JSON字段
            response_time = datetime.strptime(response_data["response_time"], "%Y-%m-%d %H:%M:%S")
            # error_details 是数组，序列化后存入JSON字段
            error_details = json.dumps(response_data["error_details"], ensure_ascii=False)
            # data 是店铺列表数组，序列化后存入JSON字段
            data_json = json.dumps(response_data["data"], ensure_ascii=False)

            params = (
                response_data.get("code", 0),
                response_data.get("message", ""),
                error_details,  # 直接用序列化后的JSON字符串
                response_time,
                data_json,  # 直接用序列化后的JSON字符串
                shop_detail.get("sid", 0),
                shop_detail.get("mid", 0),
                shop_detail.get("name", ""),
                shop_detail.get("seller_id", ""),
                shop_detail.get("account_name", ""),
                shop_detail.get("seller_account_id", 0),
                shop_detail.get("region", ""),
                shop_detail.get("country", ""),
                shop_detail.get("has_ads_setting", 0),
                shop_detail.get("marketplace_id", ""),
                shop_detail.get("status", 0)
            )

            self.cursor.execute(sql, params)
            self.conn.commit()
            print(f"成功插入/更新店铺数据：sid={shop_detail.get('sid')}, 店铺名={shop_detail.get('name')}")
        except IntegrityError as e:
            self.conn.rollback()
            raise Exception(f"数据插入失败（主键冲突等）：{str(e)}")
        except ProgrammingError as e:
            self.conn.rollback()
            raise Exception(f"SQL语法错误：{str(e)}")
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"插入店铺数据失败：{str(e)}")