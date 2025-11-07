# tests/test_upsert_stores.py
import os
import pytest
from db_utils import DBHelper

# 这组“测试用键”尽量与真实数据不冲突
TEST_SOURCE = "TESTSRC"
TEST_PLATFORM = "AMAZON"
TEST_SID = 987654321
TEST_SELLER_ID = "TEST-SELLER-XYZ"
TEST_MARKETPLACE_ID = "TEST-MARKET-US"

@pytest.fixture(scope="module")
def db():
    # 用环境变量更安全；没有就用默认（按你当前配置）
    dbh = DBHelper(
        host=os.getenv("MYSQL_HOST", "121.43.123.62"),
        port=int(os.getenv("MYSQL_PORT", "3316")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "Win2009@"),
        db_name=os.getenv("MYSQL_DB", "LXTESTN8N"),
    )
    dbh.connect()
    yield dbh
    dbh.close()

def cleanup(db):
    sql = """
    DELETE FROM stores
     WHERE (source_system=%s AND sid=%s)
        OR (platform=%s AND seller_id=%s AND marketplace_id=%s)
    """
    db.cursor.execute(sql, (TEST_SOURCE, TEST_SID, TEST_PLATFORM, TEST_SELLER_ID, TEST_MARKETPLACE_ID))
    db.conn.commit()

def row_count(db):
    sql = "SELECT COUNT(*) AS cnt FROM stores WHERE source_system=%s AND sid=%s"
    db.cursor.execute(sql, (TEST_SOURCE, TEST_SID))
    row = db.cursor.fetchone()
    if row is None:
        return 0
    return row["cnt"] if isinstance(row, dict) else row[0]


def test_upsert_idempotent(db):
    # 确保表已存在
    db.create_stores_table()
    # 清空测试数据（避免脏数据影响断言）
    cleanup(db)

    # 准备一条店铺数据
    shop = {
        "sid": TEST_SID,
        "seller_id": TEST_SELLER_ID,
        "marketplace_id": TEST_MARKETPLACE_ID,
        "name": "Test Store",
        "account_name": "acc",
        "seller_account_id": 123,
        "region": "NA",
        "country": "US",
        "has_ads_setting": 1,
        "status": 1,
    }

    # 第一次写入：应新增 1 行
    n1 = db.upsert_stores_from_api([shop], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert row_count(db) == 1
    assert n1 >= 1  # 插入通常计为1

    # 第二次同样的数据：仍应只有 1 行（走 UPDATE）
    n2 = db.upsert_stores_from_api([shop], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert row_count(db) == 1
    assert n2 >= 0  # MySQL 对 UPDATE 的“受影响行数”可能是 0 或 2（实现相关）

    # 第三次改个字段（比如 name）：仍 1 行，但应计为更新
    shop["name"] = "Test Store Renamed"
    n3 = db.upsert_stores_from_api([shop], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert row_count(db) == 1
    assert n3 >= 1

    # 清理测试数据
    cleanup(db)
