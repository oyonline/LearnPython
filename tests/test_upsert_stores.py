# tests/test_upsert_stores.py
import os
import pytest
from db_utils import DBHelper

TEST_SOURCE = "TESTSRC"
TEST_PLATFORM = "AMAZON"
TEST_SID = 99999999
TEST_SELLER_ID = "TEST-SELLER-XYZ"
TEST_MARKETPLACE_ID = "TEST-MARKET-US"

@pytest.fixture(scope="module")
def db():
    dbh = DBHelper(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        db_name=os.getenv("MYSQL_DB", "LXTESTN8N"),
    )
    dbh.connect()
    yield dbh
    dbh.close()

def cleanup(db):
    sql = """
    DELETE FROM stores
    WHERE source_system=%s AND sid=%s
       OR (platform=%s AND seller_id=%s AND marketplace_id=%s)
    """
    db.cursor.execute(sql, (TEST_SOURCE, TEST_SID, TEST_PLATFORM, TEST_SELLER_ID, TEST_MARKETPLACE_ID))
    db.conn.commit()

def count_rows(db):
    sql = """
    SELECT COUNT(*) FROM stores
     WHERE source_system=%s AND sid=%s
    """
    db.cursor.execute(sql, (TEST_SOURCE, TEST_SID))
    return db.cursor.fetchone()[0]

def test_upsert_idempotent(db):
    # 确保表存在（你在 DBHelper 里已实现）
    db.create_stores_table()

    cleanup(db)

    # 首次插入
    rows = [{
        "sid": TEST_SID,
        "seller_id": TEST_SELLER_ID,
        "marketplace_id": TEST_MARKETPLACE_ID,
        "name": "Test Store A",
        "account_name": "acc-1",
        "seller_account_id": 123,
        "region": "NA",
        "country": "US",
        "has_ads_setting": 1,
        "status": 1,
    }]
    n1 = db.upsert_stores_from_api(rows, source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_rows(db) == 1
    assert n1 >= 1

    # 第二次同样的数据（应更新0或1条，但总行数仍为1）
    n2 = db.upsert_stores_from_api(rows, source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_rows(db) == 1
    assert n2 >= 0

    # 第三次变更店铺名称（应仍为1行，但 name 更新）
    rows[0]["name"] = "Test Store A (renamed)"
    n3 = db.upsert_stores_from_api(rows, source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_rows(db) == 1
    assert n3 >= 1

    cleanup(db)
