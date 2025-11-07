import requests
import time
import urllib.parse
import json
from pathlib import Path  # 用于文件路径处理
from sign import SignBase

class OpenApiBase:
    def __init__(self, host: str, app_id: str, app_secret: str):
        self.host = host.rstrip('/')
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_cache_file = Path(f".token_cache_{app_id}.json")  # 按app_id区分缓存文件

    def _load_token_cache(self):
        """从缓存文件读取token及过期时间"""
        if self.token_cache_file.exists():
            try:
                with open(self.token_cache_file, "r") as f:
                    cache = json.load(f)
                    # 检查缓存是否有效（过期时间 > 当前时间）
                    if cache.get("expires_at", 0) > time.time():
                        return cache["access_token"]
            except (json.JSONDecodeError, KeyError):
                # 缓存文件损坏或格式错误，直接忽略
                pass
        return None

    def _save_token_cache(self, access_token, expires_in=3600):
        """保存token到缓存文件，默认有效期1小时（按领星实际过期时间调整）"""
        cache = {
            "access_token": access_token,
            "expires_at": time.time() + expires_in  # 过期时间 = 当前时间 + 有效期（秒）
        }
        with open(self.token_cache_file, "w") as f:
            json.dump(cache, f)

    def generate_access_token(self, force_refresh=False):
        """获取access_token，支持强制刷新"""
        # 1. 先尝试从缓存读取（非强制刷新时）
        if not force_refresh:
            cached_token = self._load_token_cache()
            if cached_token:
                print(f"使用缓存的access_token：{cached_token[:20]}...")
                return cached_token

        # 2. 缓存无效或强制刷新时，重新获取token
        form_data = {
            "appId": self.app_id,
            "appSecret": self.app_secret
        }
        token_url = f"{self.host}/api/auth-server/oauth/access-token"
        print(f"\n=== 获取新的access_token ===")
        print(f"请求URL: {token_url}")
        print(f"请求参数: {form_data}")

        try:
            response = requests.post(
                token_url,
                data=form_data,
                timeout=30
            )
            response.raise_for_status()
            response_data = response.json()
            print(f"响应数据: {response_data}")

            if response_data.get("code") == '200':
                access_token = response_data["data"]["access_token"]
                # 领星token通常有效期为3600秒（1小时），从返回数据提取或固定
                expires_in = response_data["data"].get("expires_in", 3600)
                self._save_token_cache(access_token, expires_in)  # 保存到缓存
                print(f"access_token获取成功（有效期{expires_in}秒）：{access_token[:20]}...")
                return access_token
            else:
                raise Exception(f"获取token失败：{response_data.get('msg')}，响应码：{response_data.get('code')}")
        except Exception as e:
            raise Exception(f"获取access_token错误：{str(e)}")

    def fetch_amazon_shop_data(self, access_token):
        """获取亚马逊店铺数据（返回完整响应，而非仅data）"""
        timestamp = str(int(time.time()))
        query_param = {
            "app_key": self.app_id,
            "access_token": access_token,
            "timestamp": timestamp,
            "page": 1,
            "page_size": 20
        }

        sign = SignBase.generate_sign(query_param, self.app_id)
        encoded_sign = urllib.parse.quote(sign)
        final_params = {**query_param, "sign": encoded_sign}

        shop_url = f"{self.host}/erp/sc/data/seller/lists"
        print(f"\n=== 获取亚马逊店铺数据 ===")
        print(f"请求URL: {shop_url}")
        print(f"请求参数: {final_params}")

        try:
            response = requests.get(
                shop_url,
                params=final_params,
                timeout=30
            )
            response.raise_for_status()
            response_data = response.json()
            print(f"响应数据: {response_data}")

            # 关键修改：返回完整响应，而非仅response_data["data"]
            if response_data.get("code") == 0:
                return response_data  # 返回完整响应
            else:
                raise Exception(
                    f"获取店铺数据失败：{response_data.get('message') or response_data.get('msg')}，响应码：{response_data.get('code')}")
        except Exception as e:
            raise Exception(f"获取店铺数据错误：{str(e)}")