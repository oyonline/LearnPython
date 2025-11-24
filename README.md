# LearnPython 数据同步脚本说明

该仓库包含一组用于从领星（LingXing）开放平台获取店铺与 FBA 库存数据并落地到 MySQL 的实用脚本。核心流程：

1. 通过 `openapi.py` 封装的 `OpenApiBase` 处理 access token 获取、接口签名（含自动重试和调试输出）以及数据分页拉取。
2. `db_utils.DBHelper` 负责建表（幂等）、批量 UPSERT 数据以及基础的连接管理。
3. `ingestion_runs_repo.IngestionRunsRepo` 将每次同步的运行记录写入 `ingestion_runs` 表，便于审计。

> **安全提示：** 仓库中的默认配置包含示例凭据，仅用于演示。生产环境请使用环境变量或配置文件覆盖。

## 目录结构速览

- `main.py`：同步店铺基础信息到 `stores` 表，并在 `original_data` 中保留原始响应。【用于店铺清单】
- `main_inventory.py`：分页同步 FBA 库存到 `inventory_fba_current` 表，支持共享仓子项展开。【用于库存快照】
- `openapi.py`：访问领星接口的工具类，包含 token 缓存、签名两种容错策略、请求调试打印。
- `db_utils.py`：数据库工具，提供 `create_stores_table`、`create_inventory_fba_current_table` 及对应的 UPSERT 方法。
- `ingestion_runs_repo.py`：运行日志表的建表与插入逻辑。
- `tests/`：针对店铺和库存 UPSERT 的幂等性校验示例（依赖可用的 MySQL 实例）。

## 环境准备

1. **Python 依赖**：脚本依赖 `requests`、`PyMySQL`、`pytest` 等第三方库，可通过 `pip install -r requirements.txt`（若无此文件则按需安装）完成。
2. **数据库**：准备一套可访问的 MySQL。示例配置指向远程实例，实际使用中应通过环境变量覆盖：
   - `MYSQL_HOST`
   - `MYSQL_PORT`
   - `MYSQL_USER`
   - `MYSQL_PASSWORD`
   - `MYSQL_DB`

## 快速运行

- 同步店铺：

  ```bash
  python main.py
  ```

- 同步 FBA 库存：

  ```bash
  python main_inventory.py
  ```

运行过程中会：
- 获取并缓存 access token（`.token_cache_<app_id>.json`）。
- 自动创建目标表（若不存在）。
- 批量 UPSERT 数据并打印受影响行数。
- 将运行结果写入 `ingestion_runs`。

## 关键实现要点

- **签名容错**：`fetch_amazon_shop_data` 先尝试原始签名，遇到 `2001006` 会切换为 URL 编码的签名重试；`fetch_inventory_fba_data` 在 query-only 和 query+body 同签之间自动切换，并打印参与签名的字段便于排查。【见 `openapi.py`】
- **幂等写入**：店铺以 `(source_system, sid)` 与 `(platform, seller_id, marketplace_id)` 唯一键保证不重复；库存以 `(source_system, sid, seller_sku, fulfillment_channel)` 唯一键覆盖更新。【见 `db_utils.py`】
- **运行日志**：无论成功或异常都会尝试写入 `ingestion_runs`，记录开始、结束时间及受影响行数或错误信息。【见 `main.py`、`main_inventory.py`、`ingestion_runs_repo.py`】

## 测试

仓库附带两个 Pytest 用例演示 UPSERT 幂等性，执行前请确保环境变量指向可用的测试数据库，并确认不会污染生产数据：

```bash
pytest tests/test_upsert_stores.py tests/test_upsert_inventory.py
```

## 故障排查提示

- 接口签名错误（code `2001006`）：开启 `openapi.py` 中的 `_debug_prepared_request` 打印，或检查 appId/appSecret 是否正确。
- 数据库连接失败：确认网络与凭据；也可在运行前导出 `MYSQL_*` 环境变量覆盖默认配置。
- 插入/更新行数为 0：检查必填字段（店铺 `sid`/`seller_id`/`marketplace_id`，库存 `seller_sku` 等）是否缺失，脚本会跳过不完整数据。
菩 提 萨 婆 诃 
