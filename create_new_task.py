from db_utils import DatabaseManager
from log_creator import log_system_action

def create_new_task():
    current_user = "admin001"  # 实际应从会话获取
    client_ip = "192.168.1.100"  # 实际应从请求上下文获取

    try:
        # 用户输入
        task_name = input("请输入任务名称: ").strip()
        resp_person = input("请输入负责人: ").strip()
        target_date = input("请输入目标完成日期 (YYYY-MM-DD): ").strip()
        priority = input("请输入优先级 (高/中/低): ").strip()  # 新增优先级输入

        # 基础验证
        if not all([task_name, resp_person, target_date, priority]):
            raise ValueError("所有必填字段不能为空")
        if priority not in ['高', '中', '低']:
            raise ValueError("优先级必须是 '高', '中', '低' 中的一个")

        with DatabaseManager() as cursor:
            sql = """
                INSERT INTO tasks 
                (task_name, responsible_person, target_completion_date, priority)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql, (task_name, resp_person, target_date, priority))  # 新增优先级参数
            new_task_id = cursor.lastrowid  # 获取新插入的task_id

            # 记录日志
            log_system_action(
                user_id=current_user,
                action_type="create_task",
                action_description=f"创建新任务: {task_name} (ID:{new_task_id})",
                ip_address=client_ip,
                device_info="Python/CLI",
                error_message=None
            )
            print(f"任务创建成功！任务ID: {new_task_id}")

    except ValueError as e:
        error_msg = f"输入验证失败: {e}"
        log_system_action(
            user_id=current_user,
            action_type="create_task",
            action_description="尝试创建任务",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=error_msg
        )
        print(error_msg)
    except Exception as e:
        log_system_action(
            user_id=current_user,
            action_type="create_task",
            action_description="创建任务",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=str(e)
        )
        print(f"任务创建异常: {e}")