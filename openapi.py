# openapi.py
import time
import urllib.parse
import json
from pathlib import Path
from sign import SignBase
from http_retry import request_json  # ✅ 超时/重试/限流封装

class OpenApiBase:
    def __init__(self, host: str, app_id: str, app_secret: str):
        self.host = host.rstrip('/')
        self.app_id = app_id
        self.app_secret = app_secret
        # 按 app_id 区分缓存文件，避免多个应用冲突
        self.token_cache_file = Path(f".token_cache_{app_id}.json")

    # ---------------- token 缓存 ----------------
    def _load_token_cache(self):
        """从本地文件读取未过期的 access_token"""
        if self.token_cache_file.exists():
            try:
                with open(self.token_cache_file, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                if cache.get("expires_at", 0) > time.time():
                    return cache["access_token"]
            except (json.JSONDecodeError, KeyError):
                pass
        return None

    def _save_token_cache(self, access_token: str, expires_in: int = 3600):
        """把 token 存到本地文件；默认有效期 3600s"""
        cache = {
            "access_token": access_token,
            "expires_at": time.time() + int(expires_in)
        }
        with open(self.token_cache_file, "w", encoding="utf-8") as f:
            json.dump(cache, f)

    # ---------------- 获取 token ----------------
    def generate_access_token(self, force_refresh: bool = False) -> str:
        """
        获取 access_token：
        - 非强制刷新优先走本地缓存
        - 否则请求 /api/auth-server/oauth/access-token
        返回：access_token 字符串
        """
        if not force_refresh:
            cached = self._load_token_cache()
            if cached:
                print(f"使用缓存的 access_token：{cached[:20]}...")
                return cached

        form_data = {
            "appId": self.app_id,
            "appSecret": self.app_secret
        }
        token_url = f"{self.host}/api/auth-server/oauth/access-token"

        print("\n=== 获取新的 access_token ===")
        print(f"请求URL: {token_url}")
        print(f"请求参数(form): {form_data}")

        try:
            # ✅ 用我们封装的 request_json 代替 requests.post
            resp = request_json(
                "POST",
                token_url,
                data=form_data,     # 领星这类多为表单提交：保持 data=，不要改成 json=
                timeout=(3, 10)    # (连接超时, 读取超时)
            )
        except Exception as e:
            raise Exception(f"获取 access_token 请求失败：{e}")

        # 容错：有的返回 code 是 '200' 字符串，有的是 200 数字
        code = str(resp.get("code"))
        if code != "200":
            msg = resp.get("message") or resp.get("msg") or ""
            raise Exception(f"获取 token 失败：code={code}, msg={msg}")

        data = resp.get("data") or {}
        access_token = data.get("access_token")
        if not access_token:
            raise Exception("获取 token 失败：响应中缺少 access_token")
        expires_in = int(data.get("expires_in", 3600))

        self._save_token_cache(access_token, expires_in)
        print(f"access_token 获取成功（有效期 {expires_in}s）：{access_token[:20]}...")
        return access_token

    # ---------------- 获取店铺列表（完整响应） ----------------
    def fetch_amazon_shop_data(self, access_token: str) -> dict:
        """
        获取亚马逊店铺数据，返回“完整响应”字典（不是只返回 data）
        你当前实现是把 token 放到 query 参数里：保持不变
        """
        timestamp = str(int(time.time()))
        query_param = {
            "app_key": self.app_id,
            "access_token": access_token,   # ✅ 你们当前走 query 传 token，就继续保留
            "timestamp": timestamp,
            "page": 1,
            "page_size": 20
        }

        # 保持你原来的签名逻辑与编码方式
        sign = SignBase.generate_sign(query_param, self.app_id)
        encoded_sign = urllib.parse.quote(sign)
        final_params = {**query_param, "sign": encoded_sign}

        shop_url = f"{self.host}/erp/sc/data/seller/lists"

        print("\n=== 获取亚马逊店铺数据 ===")
        print(f"请求URL: {shop_url}")
        print(f"请求参数(query): {final_params}")

        try:
            # ✅ 用 request_json 代替 requests.get
            resp = request_json(
                "GET",
                shop_url,
                params=final_params,
                timeout=(3, 15)  # 店铺列表一般稍慢些，读超时给长一点
            )
        except Exception as e:
            raise Exception(f"获取店铺数据请求失败：{e}")

        print(f"响应数据(截断): {str(resp)[:500]}")

        # 你之前这里判断 code == 0，我保留这个规则（接口风格常见）
        code = resp.get("code")
        ok = (code == 0) or (isinstance(code, str) and code == "0")
        if not ok:
            msg = resp.get("message") or resp.get("msg") or ""
            raise Exception(f"获取店铺数据失败：code={code}, msg={msg}")

        return resp  # 返回完整响应，方便后续留痕 + 统计
