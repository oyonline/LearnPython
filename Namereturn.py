def format_name(first, last, middle=''):
    # 处理中间名首字母缩写
    middle_initials = ' '.join([f"{name[0].upper()}." for name in middle.split()]) if middle else ''

    # 组装标准格式
    formatted = f"{last.title()}, {first.title()}"
    if middle_initials:
        formatted += f" {middle_initials.strip()}"

    return formatted




print(format_name("Mary", "Smith", "Ann"))
print(format_name("John", "Doe"))
print(format_name("David", "Jones", "Michael Roy"))