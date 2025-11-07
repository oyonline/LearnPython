import pymysql
import json  # 必须导入json模块
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

    # 重点：这个方法必须在 DBHelper 类内部！
    def insert_shop_data(self, response_data, shop_detail):
        """
        插入店铺数据到 original_data 表
        :param response_data: 接口整体响应数据（code/message等）
        :param shop_detail: 单家店铺的详情数据（sid/name等）
        """
        try:
            # 1. 准备SQL语句（字段顺序和表结构一致）
            sql = """
            INSERT INTO original_data (
                code, message, error_details, response_time, data,
                sid, mid, name, seller_id, account_name, seller_account_id,
                region, country, has_ads_setting, marketplace_id, status
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                code = VALUES(code),
                message = VALUES(message),
                error_details = VALUES(error_details),
                response_time = VALUES(response_time),
                data = VALUES(data),
                mid = VALUES(mid),
                name = VALUES(name),
                seller_id = VALUES(seller_id),
                account_name = VALUES(account_name),
                seller_account_id = VALUES(seller_account_id),
                region = VALUES(region),
                country = VALUES(country),
                has_ads_setting = VALUES(has_ads_setting),
                marketplace_id = VALUES(marketplace_id),
                status = VALUES(status)
            """

            # 2. 准备参数（对应SQL中的%s，顺序和字段一致）
            # 处理响应时间格式（接口返回是字符串，转datetime）
            response_time = datetime.strptime(response_data["response_time"], "%Y-%m-%d %H:%M:%S")
            # 错误信息是数组，转JSON字符串存入
            error_details = json.dumps(response_data["error_details"], ensure_ascii=False)
            # 关键：存储单条店铺数据（shop_detail），而非全量列表
            data_json = json.dumps(shop_detail, ensure_ascii=False)

            params = (
                # 接口整体响应参数
                response_data.get("code", 0),
                response_data.get("message", ""),
                error_details,
                response_time,
                data_json,
                # 单家店铺详情参数（从shop_detail中提取，和表字段对应）
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

            # 3. 执行SQL
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

    def create_stores_table(self):
        ddl = """
              CREATE TABLE IF NOT EXISTS stores \
              ( \
                  id \
                  BIGINT \
                  AUTO_INCREMENT \
                  PRIMARY \
                  KEY \
                  COMMENT \
                  '技术主键', \
                  source_system \
                  VARCHAR \
              ( \
                  32 \
              ) NOT NULL COMMENT '来源系统，如 LINGXING',
                  platform VARCHAR \
              ( \
                  32 \
              ) NOT NULL COMMENT '平台，如 AMAZON',
                  sid INT NOT NULL COMMENT '领星店铺ID（对企业唯一）',
                  seller_id VARCHAR \
              ( \
                  255 \
              ) NOT NULL COMMENT 'Amazon 店铺ID',
                  marketplace_id VARCHAR \
              ( \
                  255 \
              ) NOT NULL COMMENT '市场ID',
                  name VARCHAR \
              ( \
                  255 \
              ) NOT NULL COMMENT '店铺名',
                  account_name VARCHAR \
              ( \
                  255 \
              ) NOT NULL COMMENT '店铺账户名称',
                  seller_account_id INT NOT NULL COMMENT '店铺账号ID',
                  region VARCHAR \
              ( \
                  50 \
              ) NOT NULL COMMENT '站点简称（NA/…）',
                  country VARCHAR \
              ( \
                  255 \
              ) NOT NULL COMMENT '国家',
                  has_ads_setting TINYINT NOT NULL COMMENT '是否授权广告：0否/1是',
                  status TINYINT NOT NULL COMMENT '状态：0停止同步 1正常 2授权异常 3欠费停服',
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uk_source_sid \
              ( \
                  source_system, \
                  sid \
              ),
                  UNIQUE KEY uk_platform_seller_market \
              ( \
                  platform, \
                  seller_id, \
                  marketplace_id \
              ),
                  KEY idx_platform \
              ( \
                  platform \
              ),
                  KEY idx_region \
              ( \
                  region \
              ),
                  KEY idx_country \
              ( \
                  country \
              )
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='规范化店铺表'; \
              """
        try:
            self.cursor.execute(ddl)
            self.conn.commit()
            print("✅ stores 表已存在（或已创建）。")
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"创建 stores 表失败：{e}")

    def upsert_stores_from_api(self, shops, source_system="LINGXING", platform="AMAZON", chunk_size=500):
        """
        批量幂等写入 'stores'。命中任一唯一键((source_system,sid) 或 (platform,seller_id,marketplace_id))
        自动走 UPDATE，不会重复插入。
        """

        def _norm_str(v, lower=False):
            if v is None:
                s = ""
            else:
                s = str(v).strip()
            return s.lower() if lower else s

        sql = """
              INSERT INTO stores (source_system, platform, sid, seller_id, marketplace_id, \
                                  name, account_name, seller_account_id, region, country, \
                                  has_ads_setting, status, updated_at) \
              VALUES (%s, %s, %s, %s, %s, \
                      %s, %s, %s, %s, %s, \
                      %s, %s, NOW()) ON DUPLICATE KEY \
              UPDATE \
                  seller_id = \
              VALUES (seller_id), marketplace_id = \
              VALUES (marketplace_id), name = \
              VALUES (name), account_name = \
              VALUES (account_name), seller_account_id = \
              VALUES (seller_account_id), region = \
              VALUES (region), country = \
              VALUES (country), has_ads_setting = \
              VALUES (has_ads_setting), status = \
              VALUES (status), updated_at = NOW(); \
              """

        params = []
        for s in shops:
            sid = s.get("sid")
            seller_id = s.get("seller_id")
            marketplace_id = s.get("marketplace_id")
            if not sid or not seller_id or not marketplace_id:
                print(f"⚠️ 跳过：关键字段缺失 sid={sid} seller_id={seller_id} marketplace_id={marketplace_id}")
                continue
            params.append((
                _norm_str(source_system, True),
                _norm_str(platform, True),
                int(sid),
                _norm_str(seller_id, True),
                _norm_str(marketplace_id, True),
                _norm_str(s.get("name")),
                _norm_str(s.get("account_name")),
                int(s.get("seller_account_id") or 0),
                _norm_str(s.get("region")),
                _norm_str(s.get("country")),
                int(s.get("has_ads_setting") or 0),
                int(s.get("status") or 0),
            ))

        if not params:
            print("没有可写入的店铺数据。")
            return 0

        total = 0
        try:
            for i in range(0, len(params), chunk_size):
                batch = params[i:i + chunk_size]
                self.cursor.executemany(sql, batch)
                total += self.cursor.rowcount
            self.conn.commit()
            print(f"✅ UPSERT 完成（受影响行数={total}）。")
            return total
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"UPSERT stores 失败：{e}")

    # 在 db_utils.py 的 DBHelper 类中追加

    def create_inventory_fba_current_table(self):
        ddl = """
              CREATE TABLE IF NOT EXISTS inventory_fba_current \
              ( \
                  id \
                  BIGINT \
                  AUTO_INCREMENT \
                  PRIMARY \
                  KEY, \
                  source_system \
                  VARCHAR \
              ( \
                  32 \
              ) NOT NULL,
                  platform VARCHAR \
              ( \
                  32 \
              ) NOT NULL,
                  sid INT NOT NULL,
                  seller_sku VARCHAR \
              ( \
                  128 \
              ) NOT NULL,
                  sku VARCHAR \
              ( \
                  128 \
              ) NULL,
                  asin VARCHAR \
              ( \
                  32 \
              ) NULL,
                  warehouse_name VARCHAR \
              ( \
                  255 \
              ) NOT NULL,
                  fulfillment_channel VARCHAR \
              ( \
                  32 \
              ) NOT NULL,
                  share_type TINYINT NOT NULL DEFAULT 0,

                  total DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  available_total DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,

                  reserved_fc_transfers DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  reserved_fc_processing DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  reserved_customerorders DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  reserved_total DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,

                  afn_unsellable_quantity DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,

                  afn_inbound_working_quantity DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  afn_inbound_shipped_quantity DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  afn_inbound_receiving_quantity DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  stock_up_num DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,
                  inbound_total DECIMAL \
              ( \
                  18, \
                  2 \
              ) NOT NULL DEFAULT 0,

                  pulled_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
                  UNIQUE KEY uk_src_sid_sku_chan \
              ( \
                  source_system, \
                  sid, \
                  seller_sku, \
                  fulfillment_channel \
              ),
                  KEY idx_sid \
              ( \
                  sid \
              ),
                  KEY idx_seller_sku \
              ( \
                  seller_sku \
              ),
                  KEY idx_channel \
              ( \
                  fulfillment_channel \
              )
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='FBA 当前库存（覆盖式）'; \
              """
        self.cursor.execute(ddl)
        self.conn.commit()
        print("✅ inventory_fba_current 表已存在（或已创建）。")

    def upsert_inventory_fba_current_from_api(self, rows, source_system="LINGXING", platform="AMAZON", chunk_size=500):
        """
        幂等写入 FBA 最新库存：
        - 唯一键：(source_system, sid, seller_sku, fulfillment_channel)
        - 共享仓：若 share_type in (1,2) 且有 fba_storage_quantity_list，则按子项展开
        """

        def _s(v, lower=False):
            if v is None: return ""
            s = str(v).strip()
            return s.lower() if lower else s

        def _f(v):  # 数值转 float
            try:
                return float(v)
            except Exception:
                return 0.0

        def _i(v):
            try:
                return int(v)
            except Exception:
                return 0

        sql = """
              INSERT INTO inventory_fba_current (source_system, platform, sid, seller_sku, sku, asin, \
                                                 warehouse_name, fulfillment_channel, share_type, \
                                                 total, available_total, \
                                                 reserved_fc_transfers, reserved_fc_processing, reserved_customerorders, \
                                                 reserved_total, \
                                                 afn_unsellable_quantity, \
                                                 afn_inbound_working_quantity, afn_inbound_shipped_quantity, \
                                                 afn_inbound_receiving_quantity, stock_up_num, inbound_total, \
                                                 pulled_at) \
              VALUES (%s, %s, %s, %s, %s, %s, \
                      %s, %s, %s, \
                      %s, %s, \
                      %s, %s, %s, %s, \
                      %s, \
                      %s, %s, %s, %s, %s, \
                      NOW()) ON DUPLICATE KEY \
              UPDATE \
                  warehouse_name = \
              VALUES (warehouse_name), sku = \
              VALUES (sku), asin = \
              VALUES (asin), share_type = \
              VALUES (share_type), total = \
              VALUES (total), available_total = \
              VALUES (available_total), reserved_fc_transfers = \
              VALUES (reserved_fc_transfers), reserved_fc_processing = \
              VALUES (reserved_fc_processing), reserved_customerorders = \
              VALUES (reserved_customerorders), reserved_total = \
              VALUES (reserved_total), afn_unsellable_quantity = \
              VALUES (afn_unsellable_quantity), afn_inbound_working_quantity = \
              VALUES (afn_inbound_working_quantity), afn_inbound_shipped_quantity = \
              VALUES (afn_inbound_shipped_quantity), afn_inbound_receiving_quantity = \
              VALUES (afn_inbound_receiving_quantity), stock_up_num = \
              VALUES (stock_up_num), inbound_total = \
              VALUES (inbound_total), pulled_at = NOW(), updated_at = NOW(); \
              """

        params = []

        for r in rows:
            seller_sku = r.get("seller_sku")
            if not seller_sku:
                continue

            share_type = _i(r.get("share_type"))
            fc = _s(r.get("fulfillment_channel"), True)
            sku = _s(r.get("sku"), True) or None
            asin = _s(r.get("asin"), True) or None
            wh_name = _s(r.get("name"))

            # 共有数值
            total = _f(r.get("total"))
            available_total = _f(r.get("available_total"))
            reserved_fc_transfers = _f(r.get("reserved_fc_transfers"))
            reserved_fc_processing = _f(r.get("reserved_fc_processing"))
            reserved_customerorders = _f(r.get("reserved_customerorders"))
            reserved_total = reserved_fc_transfers + reserved_fc_processing + reserved_customerorders

            unsellable = _f(r.get("afn_unsellable_quantity"))

            in_working = _f(r.get("afn_inbound_working_quantity"))
            in_shipped = _f(r.get("afn_inbound_shipped_quantity"))
            in_receiving = _f(r.get("afn_inbound_receiving_quantity"))
            stock_up = _f(r.get("stock_up_num"))
            inbound_total = in_working + in_shipped + in_receiving + stock_up

            # 共享仓子项展开
            sub_list = r.get("fba_storage_quantity_list") or []
            if share_type in (1, 2) and sub_list:
                # 父项通常 sid=0，这里跳过父项
                for sub in sub_list:
                    sid = _i(sub.get("sid"))
                    if not sid:
                        continue
                    sub_wh_name = _s(sub.get("name")) or wh_name
                    q_local = _f(sub.get("quantity_for_local_fulfillment"))

                    params.append((
                        _s(source_system, True),
                        _s(platform, True),
                        sid,
                        _s(seller_sku, True),
                        sku,
                        asin,
                        sub_wh_name,
                        fc or "amazon_eu",
                        share_type,
                        q_local,  # total：对子项未知，取本地可售近似
                        q_local,  # available_total：子项可售
                        0, 0, 0, 0,  # reserved_* 置 0（无法拆分）
                        0,  # 不可售置 0
                        0, 0, 0, 0, 0  # 入库相关置 0（无法拆分）
                    ))
            else:
                # 普通仓或没有子项
                sid = _i(r.get("sid"))
                if not sid:
                    continue
                params.append((
                    _s(source_system, True),
                    _s(platform, True),
                    sid,
                    _s(seller_sku, True),
                    sku,
                    asin,
                    wh_name,
                    fc or "amazon_na",
                    share_type,
                    total,
                    available_total,
                    reserved_fc_transfers,
                    reserved_fc_processing,
                    reserved_customerorders,
                    reserved_total,
                    unsellable,
                    in_working,
                    in_shipped,
                    in_receiving,
                    stock_up,
                    inbound_total
                ))

        if not params:
            print("没有可写入的库存数据。")
            return 0

        total_affected = 0
        for i in range(0, len(params), chunk_size):
            batch = params[i:i + chunk_size]
            self.cursor.executemany(sql, batch)
            total_affected += self.cursor.rowcount
        self.conn.commit()
        print(f"✅ 库存 UPSERT 完成（受影响行数={total_affected}）。")
        return total_affected
