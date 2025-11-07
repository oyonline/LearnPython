from openapi import OpenApiBase
from db_utils import DBHelper  # 导入数据库工具类

# -------------------------- 配置信息 --------------------------
# 领星API配置
LINGXING_HOST = "https://openapi.lingxing.com"
APP_ID = "ak_wLdu8zy98S69k"
APP_SECRET = "S5K9hmRmqfC2NcPY92SMAg=="

# 数据库配置（请替换为你的数据库实际信息）
DB_HOST = "121.43.123.62"  # 数据库地址（本地是localhost）
DB_PORT = 3316  # 数据库端口（MySQL默认3306）
DB_USER = "root"  # 数据库用户名
DB_PASSWORD = "Win2009@"  # 数据库密码（必填）
DB_NAME = "LXTESTN8N"  # 数据库名（已创建）
# -----------------------------------------------------------------------------

def main():
    # 1. 初始化API客户端和数据库连接
    api = OpenApiBase(
        host=LINGXING_HOST,
        app_id=APP_ID,
        app_secret=APP_SECRET
    )

    # 初始化数据库工具类
    db_helper = DBHelper(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db_name=DB_NAME
    )

    try:
        # 2. 建立数据库连接
        db_helper.connect()

        # 3. 获取access_token（自动使用缓存）
        print("\n=== 开始获取access_token ===")
        access_token = api.generate_access_token()

        # 4. 获取亚马逊店铺数据（包含整体响应信息）
        print("\n=== 开始获取亚马逊店铺数据 ===")
        # 注意：修改openapi.py的fetch_amazon_shop_data方法，返回完整响应（而非仅data）
        full_response = api.fetch_amazon_shop_data(access_token)
        shop_list = full_response["data"]  # 从完整响应中提取店铺列表

        # 5. 循环插入每家店铺数据到数据库
        print(f"\n=== 开始插入 {len(shop_list)} 家店铺数据到数据库 ===")
        for shop in shop_list:
            db_helper.insert_shop_data(full_response, shop)

        print("\n✅ 所有店铺数据插入完成！")

    except Exception as e:
        print(f"\n❌ 执行错误：{str(e)}")
    finally:
        # 6. 无论成功失败，都关闭数据库连接
        db_helper.close()

if __name__ == "__main__":
    main()