import re
import csv
import os
import glob
from datetime import datetime

def parse_log_file(file_path):
    # 用于存储提取的数据
    extracted_data = []
    
    # 定义正则表达式模式来匹配含有账号和密码的行
    pattern = r'\[(.*?)\] (\d+\.\d+\.\d+\.\d+) (\w+) (.+)'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                # 尝试匹配含有账号和密码的行
                match = re.match(pattern, line)
                if match:
                    timestamp, ip, username, password = match.groups()
                    # 将时间从日志格式转换为标准格式
                    try:
                        dt = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
                        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # 如果时间解析失败，使用原始时间戳
                        formatted_time = timestamp
                    
                    # 添加到结果列表，同时记录来源文件
                    extracted_data.append({
                        'source_file': os.path.basename(file_path),
                        'timestamp': formatted_time,
                        'ip': ip,
                        'username': username,
                        'password': password
                    })
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    return extracted_data

def deduplicate_data(data):
    """根据用户名和密码去重，只保留第一条记录"""
    unique_data = []
    seen_credentials = set()  # 用于跟踪已经见过的用户名和密码组合
    
    for entry in data:
        # 创建用户名和密码的组合键
        credential_key = f"{entry['username']}:{entry['password']}"
        
        # 如果这个组合之前没见过，就添加到结果中
        if credential_key not in seen_credentials:
            unique_data.append(entry)
            seen_credentials.add(credential_key)
    
    return unique_data

def export_to_csv(data, output_file):
    # 如果没有数据，返回
    if not data:
        print("没有找到符合条件的数据")
        return
    
    # 定义CSV字段
    fieldnames = ['source_file', 'timestamp', 'ip', 'username', 'password']
    
    # 写入CSV文件
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"成功导出 {len(data)} 条记录到 {output_file}")

def main():
    # 获取当前目录下所有的.log文件
    log_files = glob.glob("*.log")
    
    if not log_files:
        print("当前目录中未找到.log文件")
        return
    
    # 创建以当前日期命名的输出CSV文件
    today = datetime.now().strftime('%Y%m%d%H%M%S')
    output_file = f"{today}.csv"
    
    # 用于存储所有文件中提取的数据
    all_data = []
    
    print(f"开始处理 {len(log_files)} 个日志文件...")
    
    # 处理每个日志文件
    for log_file in log_files:
        print(f"正在处理: {log_file}")
        file_data = parse_log_file(log_file)
        all_data.extend(file_data)
        print(f"从 {log_file} 中提取了 {len(file_data)} 条记录")
    
    # 去重，仅保留每个用户名和密码组合的第一条记录
    print("正在进行数据去重...")
    original_count = len(all_data)
    all_data = deduplicate_data(all_data)
    print(f"数据去重完成：从 {original_count} 条记录减少到 {len(all_data)} 条记录")
    
    # 导出所有数据到CSV
    export_to_csv(all_data, output_file)

if __name__ == "__main__":
    main()
