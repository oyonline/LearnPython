import hashlib
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

class SignBase:
    @staticmethod
    def generate_sign(query_param: dict, app_id: str) -> str:
        """按领星文档生成签名：参数ASCII排序→拼接→MD5大写→AES-ECB-PKCS5→Base64"""
        # 1. 过滤value为空的参数（value为None保留）
        filtered_params = {}
        for k, v in query_param.items():
            if v == "" or v is None:
                continue
            filtered_params[k] = str(v)

        # 2. 按ASCII排序
        sorted_items = sorted(filtered_params.items(), key=lambda x: x[0])

        # 3. 拼接成 key1=value1&key2=value2 格式
        concat_str = "&".join([f"{k}={v}" for k, v in sorted_items])
        print(f"签名-拼接字符串：{concat_str}")

        # 4. MD5加密（32位大写）
        md5 = hashlib.md5()
        md5.update(concat_str.encode("utf-8"))
        md5_str = md5.hexdigest().upper()
        print(f"签名-MD5结果：{md5_str}")

        # 5. AES-ECB-PKCS5PADDING加密（密钥为app_id，补齐16字节）
        key = app_id.ljust(16, '\0').encode("utf-8")  # AES密钥必须16/24/32字节
        cipher = AES.new(key, AES.MODE_ECB)
        padded_data = pad(md5_str.encode("utf-8"), AES.block_size, style='pkcs7')  # PKCS5兼容PKCS7
        aes_encrypted = cipher.encrypt(padded_data)

        # 6. Base64编码
        sign = base64.b64encode(aes_encrypted).decode("utf-8")
        print(f"签名-最终结果：{sign}")
        return sign