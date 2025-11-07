import pymysql
from pymysql.cursors import DictCursor


#数据库配置文件

class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root',
            'password': '1qaz2wsx',
            'database': 'test002',
            'charset': 'utf8mb4',
            'cursorclass': DictCursor
        }
        self.conn = None

    def __enter__(self):
        """ 上下文管理器入口 """
        self.conn = pymysql.connect(**self.config)
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 上下文管理器退出时自动提交/回滚并关闭连接 """
        if exc_type:  # 发生异常时回滚
            self.conn.rollback()
        else:        # 无异常则提交
            self.conn.commit()
        self.conn.close()