# openapi.py
import requests
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import quote

from sign import SignBase  # 你已有的签名工具


class OpenApiBase:
    def __init__(self, host: str, app_id: str, app_secret: str):
        self.host = host.rstrip("/")
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_cache_file = Path(f".token_cache_{app_id}.json")

    # ---------- 工具：打码 + 打印请求 ----------
    @staticmethod
    def _mask(value, left=6, right=4):
        s = "" if value is None else str(value)
        if len(s) <= left + right:
            return s
        return f"{s[:left]}***{s[-right:]}"

    @staticmethod
    def _debug_prepared_request(method, url, params, json_body, mask_keys=None, title=""):
        import requests as _req
        import json as _json
        mask_keys = mask_keys or []
        params_print = dict(params or {})
        for k in mask_keys:
            if k in params_print:
                params_print[k] = OpenApiBase._mask(params_print[k])

        print("\n====== DEBUG 请求构造", title, "======")
        print("Raw URL:", url)
        print("Query params (masked):", params_print)
        print("JSON body:", _json.dumps(json_body or {}, ensure_ascii=False))

        req = _req.Request(method=method.upper(), url=url, params=params, json=json_body,
                           headers={"Content-Type": "application/json; charset=utf-8"})
        prep = req.prepare()
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
        pu = urlparse(prep.url)
        q = dict(parse_qsl(pu.query, keep_blank_values=True))
        for k in mask_keys:
            if k in q:
                q[k] = OpenApiBase._mask(q[k])
        masked_url = urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, urlencode(q), pu.fragment))
        print("Prepared URL (masked):", masked_url)

        body_text = prep.body
        try:
            if isinstance(body_text, bytes):
                body_text = body_text.decode("utf-8", errors="ignore")
        except Exception:
            pass
        print("Prepared Body:", body_text)
        print("====================================\n")

    # ---------- token 缓存 ----------
    def _load_token_cache(self) -> Optional[str]:
        if self.token_cache_file.exists():
            try:
                with open(self.token_cache_file, "r") as f:
                    cache = json.load(f)
                    if cache.get("expires_at", 0) > time.time():
                        return cache["access_token"]
            except Exception:
                pass
        return None

    def _save_token_cache(self, access_token: str, expires_in: int = 3600) -> None:
        cache = {
            "access_token": access_token,
            "expires_at": time.time() + int(expires_in)
        }
        with open(self.token_cache_file, "w") as f:
            json.dump(cache, f)

    # ---------- 获取 access_token ----------
    def generate_access_token(self, force_refresh: bool = False) -> str:
        if not force_refresh:
            cached = self._load_token_cache()
            if cached:
                print(f"使用缓存 access_token：{cached[:20]}...")
                return cached

        url = f"{self.host}/api/auth-server/oauth/access-token"
        form_data = {"appId": self.app_id, "appSecret": self.app_secret}
        print("\n=== 获取新的 access_token ===")
        print("请求URL:", url)
        print("请求参数:", form_data)

        resp = requests.post(url, data=form_data, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        code = data.get("code")
        ok = (code == 200) or (isinstance(code, str) and code == "200")
        if not ok:
            raise RuntimeError(f"get token failed: code={code}, msg={data.get('message') or data.get('msg')}")
        token = data["data"]["access_token"]
        expires_in = data["data"].get("expires_in", 3600)
        self._save_token_cache(token, expires_in)
        print(f"access_token获取成功（有效期{expires_in}s）：{token[:20]}...")
        return token

    # ---------- 店铺列表（自动分页 + 签名容错：raw → urlencoded） ----------
    def fetch_amazon_shop_data(self, access_token: str, page_size: int = 100) -> Dict[str, Any]:
        """
        GET /erp/sc/data/seller/lists
        - 自动分页：page 从 1 开始，直到返回数量 < page_size
        - 签名容错：先用 raw sign；若 code=2001006（签名错误）→ 再用 urlencode(sign)
        - 返回：完整响应 dict（其中 data 是聚合后的店铺列表），与 main.py 兼容
        """
        url = f"{self.host}/erp/sc/data/seller/lists"
        all_rows: List[Dict[str, Any]] = []
        page = 1
        page_size = max(20, min(200, int(page_size)))

        while True:
            ts = str(int(time.time()))
            # 很多对接方把分页字段也纳入签名；沿用你先前的做法
            query = {
                "app_key": self.app_id,
                "access_token": access_token,
                "timestamp": ts,
                "page": page,
                "page_size": page_size,
            }

            # 方案A：raw sign（不额外 urlencode）
            sign_a = SignBase.generate_sign(query, self.app_id)
            params_a = dict(query)
            params_a["sign"] = sign_a

            # 可按需打开调试
            # self._debug_prepared_request("GET", url, params_a, None, mask_keys=["access_token", "sign"],
            #                              title=f"店铺列表 A page={page}")

            try:
                r = requests.get(url, params=params_a, timeout=20)
                r.raise_for_status()
                j = r.json()
                code = j.get("code")
                ok = (code == 0) or (isinstance(code, str) and code == "0")
                if ok:
                    rows = j.get("data") or []
                    all_rows.extend(rows)
                    print(f"[SHOPS] page={page}, got={len(rows)} (A:raw-sign)")
                    if not rows or len(rows) < page_size:
                        break
                    page += 1
                    continue
                else:
                    msg = j.get("message") or j.get("msg") or ""
                    if str(code) != "2001006":
                        raise RuntimeError(f"拉店铺失败：code={code}, msg={msg}")
                    print("[SHOPS] code=2001006，改用 urlencode(sign) 重试。")
            except Exception as e:
                # 网络/解析异常：这里直接抛错；也可根据需要切到B
                if "2001006" not in str(e):
                    raise RuntimeError(f"拉店铺失败（A）：{e}")

            # 方案B：对 sign 做 URL 编码（与你先前成功的写法一致）
            sign_b = SignBase.generate_sign(query, self.app_id)
            params_b = dict(query)
            params_b["sign"] = quote(sign_b)  # 只编码 sign

            # 可按需打开调试
            # self._debug_prepared_request("GET", url, params_b, None, mask_keys=["access_token", "sign"],
            #                              title=f"店铺列表 B page={page}")

            r = requests.get(url, params=params_b, timeout=20)
            r.raise_for_status()
            j = r.json()
            code = j.get("code")
            ok = (code == 0) or (isinstance(code, str) and code == "0")
            if not ok:
                msg = j.get("message") or j.get("msg") or ""
                raise RuntimeError(f"拉店铺失败：code={code}, msg={msg}")
            rows = j.get("data") or []
            all_rows.extend(rows)
            print(f"[SHOPS] page={page}, got={len(rows)} (B:urlencoded-sign)")
            if not rows or len(rows) < page_size:
                break
            page += 1

        # 组装一个“完整响应”返回（与 main.py 当前用法兼容）
        resp = {
            "code": 0,
            "message": "success",
            "error_details": [],
            "request_id": "",
            "response_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "data": all_rows,
            "total": len(all_rows),
        }
        return resp

    # ---------- FBA 库存（分页 + 两种签名策略 + Debug 打印） ----------
    def fetch_inventory_fba_data(
        self,
        access_token: str,
        length: int = 200,
        extra_filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        POST /basicOpen/openapi/storage/fbaWarehouseDetail
        自动重试：先“只签 query”，若 2001006 再切到“query+body 同签（布尔小写化参与签名）”。
        同时打印完整请求入参（对 access_token/sign 打码）。
        """
        def _normalize_for_sign(v):
            if isinstance(v, bool):
                return "true" if v else "false"
            if v is None:
                return ""
            return str(v)

        url = f"{self.host}/basicOpen/openapi/storage/fbaWarehouseDetail"
        all_rows: List[Dict[str, Any]] = []
        offset = 0
        length = max(20, min(200, int(length)))

        while True:
            ts = str(int(time.time()))
            # 基础 query
            query = {
                "app_key": self.app_id,
                "access_token": access_token,
                "timestamp": ts
            }
            # body（分页 + 你的筛选）
            body: Dict[str, Any] = {
                "offset": offset,
                "length": length,
                "is_hide_zero_stock": "0",
                # "query_fba_storage_quantity_list": True,
            }
            if extra_filters:
                body.update(extra_filters)

            # 策略 A：只签 query
            sign_a = SignBase.generate_sign(query, self.app_id)
            params_a = dict(query)
            params_a["sign"] = sign_a

            self._debug_prepared_request(
                "POST", url, params_a, body,
                mask_keys=["access_token", "sign"],
                title="FBA库存 策略A（只签query）"
            )

            try:
                r = requests.post(url, params=params_a, json=body, timeout=20)
                r.raise_for_status()
                j = r.json()
                code = j.get("code")
                ok = (code == 0) or (isinstance(code, str) and code == "0")
                if ok:
                    rows = j.get("data") or []
                    all_rows.extend(rows)
                    print(f"[INV] offset={offset}, got={len(rows)} (A:query-only-sign)")
                    if not rows or len(rows) < length:
                        break
                    offset += length
                    continue
                else:
                    msg = j.get("message") or j.get("msg") or ""
                    if str(code) != "2001006":
                        raise RuntimeError(f"拉库存失败：code={code}, msg={msg}")
                    print("[INV] code=2001006，切换为“query+body 一起签名”重试。")
            except Exception as e:
                if "2001006" not in str(e):
                    raise RuntimeError(f"拉库存失败（A）：{e}")

            # 策略 B：query+body 同签（布尔小写化）
            body_for_sign = {k: _normalize_for_sign(v) for k, v in body.items()}
            sign_b_params = dict(query)
            sign_b_params.update(body_for_sign)
            sign_b = SignBase.generate_sign(sign_b_params, self.app_id)
            params_b = dict(query)
            params_b["sign"] = sign_b

            print("参与签名（B）字段：", {k: sign_b_params[k] for k in sorted(sign_b_params.keys())})
            self._debug_prepared_request(
                "POST", url, params_b, body,
                mask_keys=["access_token", "sign"],
                title="FBA库存 策略B（query+body同时签）"
            )

            try:
                r = requests.post(url, params=params_b, json=body, timeout=20)
                r.raise_for_status()
                j = r.json()
            except Exception as e:
                raise RuntimeError(f"拉库存失败（B）：{e}")

            code = j.get("code")
            ok = (code == 0) or (isinstance(code, str) and code == "0")
            if not ok:
                msg = j.get("message") or j.get("msg") or ""
                raise RuntimeError(f"拉库存失败：code={code}, msg={msg}")

            rows = j.get("data") or []
            all_rows.extend(rows)
            print(f"[INV] offset={offset}, got={len(rows)} (B:query+body-sign)")

            if not rows or len(rows) < length:
                break
            offset += length

        print(f"[INV] 合并库存行数：{len(all_rows)}")
        return all_rows
