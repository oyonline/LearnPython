from db_utils import DatabaseManager
import datetime


def log_system_action(
        user_id: str,
        action_type: str,
        action_description: str,
        ip_address: str,
        device_info: str = None,
        error_message: str = None
):
    """
    记录系统操作日志
    :param user_id: 操作用户ID
    :param action_type: 操作类型 (login/update/delete等)
    :param action_description: 操作详细描述
    :param ip_address: 客户端IP地址
    :param device_info: 客户端设备信息
    :param error_message: 错误信息（操作失败时需传入）
    """
    try:
        with DatabaseManager() as cursor:
            sql = """
                INSERT INTO system_log (
                    user_id,
                    action_type,
                    action_description,
                    ip_address,
                    device_info,
                    result_status,
                    error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # 自动确定操作结果状态
            result_status = 'failure' if error_message else 'success'

            cursor.execute(sql, (
                user_id,
                action_type[:30],  # 确保不超过字段长度
                action_description,
                ip_address[:45],  # 符合IPv6最大长度
                device_info[:255] if device_info else None,
                result_status,
                error_message
            ))
    except Exception as e:
        print(f"日志记录失败: {str(e)}")
