import os
import pandas as pd
import glob
from datetime import datetime

def deduplicate_csv_files(directory_path):
    # 获取目录下所有的CSV文件
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
    
    if not csv_files:
        print(f"在目录 {directory_path} 中没有找到CSV文件")
        return
    
    # 创建一个空的DataFrame，用于存储所有文件的数据
    all_data = pd.DataFrame()
    
    # 读取所有CSV文件并合并数据
    for file in csv_files:
        try:
            # 假设CSV文件包含表头 source_file,timestamp,ip,username,password
            df = pd.read_csv(file)
            
            # 确保文件包含所需的列
            required_columns = ['source_file', 'timestamp', 'ip', 'username', 'password']
            if not all(column in df.columns for column in required_columns):
                print(f"文件 {file} 缺少必要的列，已跳过")
                continue
                
            # 添加到总数据集
            all_data = pd.concat([all_data, df], ignore_index=True)
        except Exception as e:
            print(f"处理文件 {file} 时出错: {str(e)}")
    
    if all_data.empty:
        print("没有有效的数据可处理")
        return
    
    # 将timestamp列转换为datetime格式以便排序
    try:
        all_data['timestamp'] = pd.to_datetime(all_data['timestamp'])
    except Exception as e:
        print(f"转换timestamp列时出错: {str(e)}")
        print("尝试继续处理，但结果可能不准确")
    
    # 按username和password分组，然后按timestamp排序并保留最早的记录
    deduplicated_data = all_data.sort_values('timestamp').drop_duplicates(
        subset=['username', 'password'], keep='first')
    
    # 保存去重后的数据
    output_file = os.path.join(directory_path, "deduplicated_data.csv")
    deduplicated_data.to_csv(output_file, index=False)
    
    print(f"处理完成。共处理 {len(all_data)} 条记录，去重后保留 {len(deduplicated_data)} 条记录。")
    print(f"去重后的数据已保存至 {output_file}")

if __name__ == "__main__":
    # 请修改为你的CSV文件所在的目录路径
    directory_path = "."
    deduplicate_csv_files(directory_path)