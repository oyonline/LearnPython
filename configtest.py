from db_utils import DatabaseManager
from log_creator import log_system_action



# 硬编码上下文信息（需替换为实际获取逻辑）
current_user = "admin001"  # 实际应从会话获取
client_ip = "192.168.1.100"  # 实际应从请求上下文获取
device_information = "Python"




#-------------增
def insert_employee_data():
    # 用户输入数据（示例）
    id = int(input("请输入员工ID: "))
    name = input("请输入员工姓名: ")
    money = int(input("请输入员工月薪: "))

    conn = None
    try:
        with DatabaseManager() as cursor:
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
        target_id = int(input("请输入要删除的ID: "))

        with DatabaseManager() as cursor:
            sql = "DELETE FROM account WHERE id = %s"
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


#-------------改
def update_account_balance():
    try:
        # 获取用户输入
        target_id = int(input("请输入要修改的员工ID: "))
        new_name = input("请输入新的名字: ")
        new_balance = int(input("请输入新的薪资数额: "))

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






