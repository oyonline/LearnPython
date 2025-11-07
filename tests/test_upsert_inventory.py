# tests/test_upsert_inventory.py
import os
import pytest
from db_utils import DBHelper

TEST_SOURCE = "TESTSRC"
TEST_PLATFORM = "AMAZON"
TEST_SID = 999001
TEST_SELLER_SKU = "PYTEST-SKU-01"
TEST_FC = "AMAZON_NA"

@pytest.fixture(scope="module")
def db():
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
    DELETE FROM inventory_fba_current
    WHERE source_system=%s AND sid=%s AND seller_sku=%s AND fulfillment_channel=%s
    """
    db.cursor.execute(sql, (TEST_SOURCE.lower(), TEST_SID, TEST_SELLER_SKU.lower(), TEST_FC.lower()))
    db.conn.commit()

def count_row(db):
    sql = """
    SELECT COUNT(*) AS cnt FROM inventory_fba_current
    WHERE source_system=%s AND sid=%s AND seller_sku=%s AND fulfillment_channel=%s
    """
    db.cursor.execute(sql, (TEST_SOURCE.lower(), TEST_SID, TEST_SELLER_SKU.lower(), TEST_FC.lower()))
    row = db.cursor.fetchone()
    return row["cnt"] if isinstance(row, dict) else row[0]

def test_upsert_inventory_idempotent(db):
    db.create_inventory_fba_current_table()
    cleanup(db)

    row = {
        "sid": TEST_SID,
        "name": "美国仓-测试",
        "seller_sku": TEST_SELLER_SKU,
        "sku": "INNER-SKU-01",
        "asin": "B000TEST01",
        "fulfillment_channel": TEST_FC,
        "share_type": 0,
        "total": 10,
        "available_total": 8,
        "reserved_fc_transfers": 1,
        "reserved_fc_processing": 1,
        "reserved_customerorders": 0,
        "afn_unsellable_quantity": 0,
        "afn_inbound_working_quantity": 2,
        "afn_inbound_shipped_quantity": 1,
        "afn_inbound_receiving_quantity": 0,
        "stock_up_num": 0
    }

    n1 = db.upsert_inventory_fba_current_from_api([row], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_row(db) == 1
    assert n1 >= 1

    n2 = db.upsert_inventory_fba_current_from_api([row], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_row(db) == 1
    assert n2 >= 0

    # 修改一个字段，再次 UPSERT
    row["available_total"] = 7
    n3 = db.upsert_inventory_fba_current_from_api([row], source_system=TEST_SOURCE, platform=TEST_PLATFORM)
    assert count_row(db) == 1
    assert n3 >= 1

    cleanup(db)
