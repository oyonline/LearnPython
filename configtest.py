from db_utils_old import DatabaseManager
from log_creator import log_system_action



# 硬编码上下文信息（需替换为实际获取逻辑）
current_user = "admin001"  # 实际应从会话获取
client_ip = "192.168.1.100"  # 实际应从请求上下文获取
device_information = "Python"

# ---------------- 通用输入验证函数
def get_valid_input(prompt: str, expected_type: type,
                    min_val=None, max_val=None,
                    max_retry: int = 3) -> any:
    """
    带验证的通用输入函数
    :param prompt: 输入提示语
    :param expected_type: 期望的数据类型（int/str等）
    :param min_val: 最小值（仅数值类型有效）
    :param max_val: 最大值（仅数值类型有效）
    :param max_retry: 最大重试次数
    :return: 验证通过的值
    """
    for attempt in range(1, max_retry + 1):
        try:
            value = input(prompt)

            # 空值检查
            if not value.strip():
                raise ValueError("输入不能为空")

            # 类型转换
            converted_value = expected_type(value)

            # 数值范围检查
            if issubclass(expected_type, (int, float)):
                if min_val is not None and converted_value < min_val:
                    raise ValueError(f"值不能小于 {min_val}")
                if max_val is not None and converted_value > max_val:
                    raise ValueError(f"值不能超过 {max_val}")

            return converted_value

        except ValueError as e:
            print(f"输入错误: {str(e)}")
            if attempt == max_retry:
                raise ValueError(f"超过最大重试次数（{max_retry}次）")
            print(f"请重新输入（剩余尝试次数：{max_retry - attempt}）")


#-------------增
def insert_employee_data():

    try:
        with DatabaseManager() as cursor:

            # 带验证的输入
            id = get_valid_input("请输入员工ID: ", int, min_val=1)
            name = get_valid_input("请输入员工姓名: ", str).strip()
            money = get_valid_input("请输入月薪（元）: ", int, min_val=0)


            # 使用三引号规范SQL格式（保持缩进对齐）
            sql = """
                INSERT INTO account 
                (id, name, money)
                VALUES (%s, %s, %s)
            """

            # 3. 执行插入操作[3](@ref)
            cursor.execute(sql, (id,name, money))
            print(f"成功插入 {cursor.rowcount} 条记录")

            #日志记录
            log_system_action(
                user_id=current_user,
                action_type="insert_employee_data",
                action_description=f"添加了员工 {id} {name} 的薪资为 {money}",
                ip_address=client_ip,
                device_info = device_information,
                error_message=""
            )
    except ValueError:
        error_msg = "输入格式错误"
        log_system_action(
            user_id=current_user,
            action_type="insert",
            action_description="尝试新增员工记录",
            ip_address=client_ip,
            device_info = device_information,
            error_message=error_msg
        )
        print("请输入有效的数字格式")
    except Exception as e:
        error_msg = str(e)
        log_system_action(
            user_id=current_user,
            action_type="insert",
            action_description="新增员工记录",
            ip_address=client_ip,
            device_info = device_information,
            error_message=error_msg
        )
        print(f"插入异常: {error_msg}")



#-------------删  改进版带输入提示的版本
def delete_account_by_id():
    try:
        target_id = get_valid_input("请输入要删除的ID: ", int, min_val=1)

        with DatabaseManager() as cursor:
            sql = "DELETE FROM account WHERE id = %s AND is_deletable = 1"  # 防止误删核心数据
            cursor.execute(sql, (target_id,))
            print(f"成功删除 {cursor.rowcount} 条记录")

            # 新增成功日志
            log_system_action(
                user_id=current_user,
                action_type="account_delete",
                action_description=f"删除员工ID: {target_id}",
                ip_address=client_ip,
                device_info="Python/CLI",
                error_message=""
            )

    except ValueError:
        error_msg = "无效的ID格式"
        log_system_action(
            user_id=current_user,
            action_type="delete",
            action_description="尝试删除员工记录",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=error_msg
        )
        print("请输入有效的数字ID")
    except Exception as e:
        error_msg = str(e)
        log_system_action(
            user_id=current_user,
            action_type="delete",
            action_description="删除员工记录",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=error_msg
        )
        print(f"删除异常: {error_msg}")


#-------------改 - 更新
def update_account_balance():
    try:
        # 获取用户输入
        target_id = get_valid_input("输入要修改的员工ID ", int, min_val=1)
        new_name = get_valid_input("请输入新的名字: ", str).strip()
        new_balance = get_valid_input("请输入新的薪资数额: ", int, min_val=0)

        with DatabaseManager() as cursor:
            sql = """
                UPDATE account
                SET money = %s,name = %s
                WHERE id = %s
            """
            cursor.execute(sql, (new_balance,new_name, target_id))
            print(f"成功更新 {cursor.rowcount} 条记录")  # 将打印移到 with 块内

            log_system_action(
                user_id=current_user,
                action_type="update_account_balance",
                action_description=f"修改了员工 {target_id} 的薪资和姓名为 {new_balance,new_name}",
                ip_address=client_ip,
                device_info="",
                error_message=""
            )
            print(f"成功更新 {cursor.rowcount} 条记录")

    except ValueError:
        error_msg = "输入格式错误"
        log_system_action(
            user_id=current_user,
            action_type="update",
            action_description="尝试更新薪资记录",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=error_msg
        )
    except Exception as e:
        log_system_action(
            user_id=current_user,
            action_type="update",
            action_description="更新薪资记录",
            ip_address=client_ip,
            device_info="Python/CLI",
            error_message=str(e)
        )
        print(f"更新异常: {e}")





#-------------查
def query_data():
    conn = None  # 初始化连接对象避免 finally 块报错
    try:
        with DatabaseManager() as cursor:
            # 使用三引号规范SQL格式（保持缩进对齐）
            sql = """
                                    SELECT 
                                        gender,
                                        COUNT(*) AS total,
                                        ROUND(COUNT(*) * 100.0 / (
                                            SELECT COUNT(*) 
                                            FROM employees 
                                            WHERE age > 20 AND age < 45
                                        ), 2) AS percentage
                                    FROM employees
                                    WHERE age > 20 AND age < 45
                                    GROUP BY gender;
                        """
            cursor.execute(sql)
            # 获取并格式化结果
            results = cursor.fetchall()
            for row in results:
                print(f"性别: {row['gender']}, 人数: {row['total']}, 占比: {row['percentage']}%")
    except Exception as e:
        print(f"查询异常: {e}")






