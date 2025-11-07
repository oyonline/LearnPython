# main_inventory.py
from datetime import datetime
from openapi import OpenApiBase
from db_utils import DBHelper
from ingestion_runs_repo import IngestionRunsRepo, IngestionRun

# ===================== 配置 =====================
LINGXING_HOST = "https://openapi.lingxing.com"
APP_ID = "ak_wLdu8zy98S69k"
APP_SECRET = "S5K9hmRmqfC2NcPY92SMAg=="

DB_HOST = "121.43.123.62"
DB_PORT = 3316
DB_USER = "root"
DB_PASSWORD = "Win2009@"
DB_NAME = "LXTESTN8N"
# =================================================

def main():
    JOB_NAME = "sync_inventory_from_lingxing"
    started_at = datetime.now()

    api = OpenApiBase(LINGXING_HOST, APP_ID, APP_SECRET)
    db = DBHelper(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)

    success = 0
    fail = 0
    note = "init"

    # 1) 连接数据库（失败就退出，避免后续 AttributeError）
    try:
        db.connect()
        print("数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败：{e}")
        try:
            repo = IngestionRunsRepo(db)
            repo.ensure_table()
            repo.insert_run(IngestionRun(
                job_name=JOB_NAME,
                started_at=started_at,
                ended_at=datetime.now(),
                success_count=0,
                fail_count=1,
                note=f"db connect error: {e}"
            ))
        except Exception as e2:
            print(f"⚠️ 写运行记录失败（忽略）：{e2}")
        finally:
            db.close()
        return

    repo = IngestionRunsRepo(db)
    try:
        repo.ensure_table()
    except Exception as e:
        print(f"⚠️ 日志表 ensure 失败（继续执行）：{e}")

    try:
        # 2) access_token
        print("\n=== 获取 access_token ===")
        token = api.generate_access_token()

        # 3) 拉库存（可按需加筛选条件）
        print("\n=== 分页拉取 FBA 库存 ===")
        rows = api.fetch_inventory_fba_data(
            token,
            length=200,
            extra_filters={
                "is_hide_zero_stock": "0",
                # 需要时再开启进一步筛选：
                # "fulfillment_channel_type": "FBA",
                # "status": "1",
                #"search_field": "seller_sku",
                #"search_value": "AMKK-KTATLSSTS-6COOR-C-US-FBA",
            }
        )
        total_rows = len(rows)
        print(f"✅ 库存拉取完成，共 {total_rows} 行")

        # 4) 入库（UPSERT）
        affected = 0
        if total_rows > 0:
            affected = db.upsert_inventory_fba_current_from_api(
                rows, source_system="LINGXING", platform="AMAZON"
            )
        print(f"✅ 入库完成：affected={affected}, rows={total_rows}")

        success = affected
        fail = 0
        note = f"rows={total_rows}; affected={affected}"

    except Exception as e:
        fail = 1
        note = f"error: {e}"
        print(f"❌ 执行错误：{e}")

    finally:
        # 5) 写运行记录（无论成功失败）
        try:
            repo.insert_run(IngestionRun(
                job_name=JOB_NAME,
                started_at=started_at,
                ended_at=datetime.now(),
                success_count=success,
                fail_count=fail,
                note=note
            ))
            print(f"📝 已写入运行记录：{JOB_NAME} | {started_at} → {datetime.now()} | {note}")
        except Exception as e:
            print(f"⚠️ 写运行记录失败（略过）：{e}")
        db.close()
        print("数据库连接已关闭")


if __name__ == "__main__":
    main()
