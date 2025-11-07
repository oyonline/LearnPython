from datetime import datetime

from db_utils_old import DatabaseManager
from log_creator import log_system_action

def update_task():
    current_user = "admin001"  # 实际应从会话获取
    client_ip = "192.168.1.100"  # 实际应从请求上下文获取

    try:
        task_id = int(input("请输入要更新的任务ID: "))
        new_status = input("请输入新状态 (Not Started/In Progress/Completed): ").strip()
        actual_date = None

        # 状态为Completed时自动记录当前时间
        if new_status.lower() == "completed":
            actual_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        priority = input("请输入新优先级 ('Low','Medium','High'): ").strip()  # 新增优先级输入
        if priority and priority not in ['Low','Medium','High']:
            raise ValueError("优先级必须是 'Low','Medium','High' 中的一个")

        with DatabaseManager() as cursor:
            # 动态构建更新SQL
            sql = """
                UPDATE tasks 
                SET completion_status = %s,
                    actual_completion_date = %s,
                    priority = %s  # 新增优先级更新
                WHERE task_id = %s
            """
            cursor.execute(sql, (new_status, actual_date, priority, task_id))  # 新增优先级参数

            if cursor.rowcount == 0:
                raise ValueError(f"任务ID {task_id} 不存在")

            # 记录日志
            log_system_action(
                user_id=current_user,
                action_type="update_task",
                action_description=f"更新任务ID {task_id} 状态为 {new_status}",
                ip_address=client_ip,
                device_info="Python/CLI",
                error_message=None
            )
            print("任务更新成功")

    except ValueError as e:
        error_msg = f"输入错误: {e}"
        log_system_action(
            user_id=current_user,
            action_type="update_task",
            action_description="尝试更新任务",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=error_msg
        )
        print(error_msg)
    except Exception as e:
        log_system_action(
            user_id=current_user,
            action_type="update_task",
            action_description="更新任务",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=str(e)
        )
        print(f"任务更新异常: {e}")