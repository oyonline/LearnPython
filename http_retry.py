# http_retry.py
import time
import threading
from typing import Any, Dict, Optional, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class SimpleRateLimiter:
    """非常轻量：保证两次请求至少间隔 min_interval 秒。"""
    def __init__(self, min_interval: float = 0.2):
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.time()

def build_resilient_session(
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist: Optional[list[int]] = None,
    allowed_methods: Optional[list[str]] = None,
) -> requests.Session:
    """返回带自动重试的 Session，对 429/5xx 等状态码做指数退避。"""
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]
    if allowed_methods is None:
        allowed_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

    sess = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(m.upper() for m in allowed_methods),
        raise_on_status=False,   # 不在 urllib3 层抛异常，交给 requests.raise_for_status()
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

# 全局可复用的 session 与限流器（按需修改速率）
session = build_resilient_session()
rate_limiter = SimpleRateLimiter(min_interval=0.2)  # 5 QPS 左右；需要更严就调大一点

def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, Any], str, bytes]] = None,
    timeout: tuple[float, float] = (3.0, 10.0),  # (连接超时, 读取超时)
    use_rate_limit: bool = True,
) -> Dict[str, Any]:
    """带超时/重试/限流的统一请求入口，返回 .json() 解析后的字典。"""
    if use_rate_limit:
        rate_limiter.wait()
    resp = session.request(
        method=method.upper(),
        url=url,
        headers=headers,
        params=params,
        json=json,
        data=data,
        timeout=timeout,
    )
    # 429/5xx 已在 urllib3 层重试过；这里再严格兜底
    resp.raise_for_status()
    # 尝试 JSON 解析
    try:
        return resp.json()
    except ValueError as e:
        raise RuntimeError(f"响应非 JSON 或解析失败: {e}; text={resp.text[:500]!r}")
