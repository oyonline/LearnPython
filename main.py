# main.py
from datetime import datetime
from openapi import OpenApiBase
from db_utils import DBHelper
from ingestion_runs_repo import IngestionRunsRepo, IngestionRun

# -------------------------- 配置信息 --------------------------
# 领星API配置
LINGXING_HOST = "https://openapi.lingxing.com"
APP_ID = "ak_wLdu8zy98S69k"
APP_SECRET = "S5K9hmRmqfC2NcPY92SMAg=="

# 数据库配置（请替换为你的数据库实际信息）
DB_HOST = "121.43.123.62"  # 数据库地址（本地是localhost）
DB_PORT = 3316             # 数据库端口（MySQL默认3306）
DB_USER = "root"           # 数据库用户名
DB_PASSWORD = "Win2009@"   # 数据库密码
DB_NAME = "LXTESTN8N"      # 数据库名（已创建）
# -------------------------------------------------------------

def main():
    JOB_NAME = "sync_stores_from_lingxing"

    # 1) 初始化
    api = OpenApiBase(host=LINGXING_HOST, app_id=APP_ID, app_secret=APP_SECRET)
    db_helper = DBHelper(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db_name=DB_NAME)

    # 运行记录字段（异常也有值）
    started_at = datetime.now()
    ended_at = started_at
    affected = 0            # upsert 受影响的行数
    note = None
    success_count = 0       # 这里用 affected 作为成功数
    fail_count = 0

    try:
        # 2) 建立数据库连接
        db_helper.connect()

        # 3) 获取 access_token（含缓存/超时/重试）
        print("\n=== 开始获取access_token ===")
        access_token = api.generate_access_token()

        # 4) 拉取店铺完整响应
        print("\n=== 开始获取亚马逊店铺数据 ===")
        full_response = api.fetch_amazon_shop_data(access_token)
        shop_list = full_response.get("data", [])
        print(f"✅ 拉到店铺数：{len(shop_list)}")

        # 5) 原有 ODS 留痕（逐条写 original_data）
        print(f"\n=== 插入 {len(shop_list)} 家店铺到 original_data（留痕） ===")
        for shop in shop_list:
            db_helper.insert_shop_data(full_response, shop)

        # 6) 规范层：建表（幂等）+ 批量 UPSERT（幂等）
        db_helper.create_stores_table()
        print("\n=== 批量 UPSERT 到 stores（幂等） ===")
        affected = db_helper.upsert_stores_from_api(
            shop_list,
            source_system="LINGXING",
            platform="AMAZON"
        )
        print(f"✅ stores UPSERT 受影响行数 = {affected}")

        success_count = affected
        note = f"affected={affected}; shops={len(shop_list)}"
        print("\n✅ ODS + DIM 两条支线完成！")

    except Exception as e:
        fail_count = 1
        note = f"error: {e}"
        print(f"\n❌ 执行错误：{e}")

    finally:
        ended_at = datetime.now()
        # 7) 写入运行日志（确保表存在）
        try:
            repo = IngestionRunsRepo(db_helper)
            repo.ensure_table()
            repo.insert_run(IngestionRun(
                job_name=JOB_NAME,
                started_at=started_at,
                ended_at=ended_at,
                success_count=success_count,
                fail_count=fail_count,
                note=note
            ))
            print(f"📝 已写入运行记录：{JOB_NAME} | {started_at} → {ended_at} | {note}")
        except Exception as e2:
            print(f"⚠️ 写入运行记录失败：{e2}")
        finally:
            db_helper.close()

if __name__ == "__main__":
    main()
