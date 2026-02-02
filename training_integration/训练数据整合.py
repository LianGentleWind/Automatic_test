import pandas as pd
import os

# --- 配置区域 ---
# 存放 CSV 文件的目录路径
SOURCE_DIRECTORY = './raw_data/training/' 
# 输出的文件名
OUTPUT_FILE = './data/training/aggregated_data_report.xlsx'

# 指定的关键字段名列表（保持你提供的顺序）
TARGET_FIELDS = [
    'network0_size', 'num_procs', 'pipeline_par', 'data_par', 
    'expert_par', 'tensor_par', 'fw_time', 'bw_time', 
    'matrix_layers_time', 'vector_layers_time', 'optim_step_time', 
    'recompute_time', 'bubble_time', 'tp_comm_link_time', 
    'ep_comm_link_time', 'pp_comm_link_time', 'dp_comm_link_time', 
    'ep_comm_exposed_time', 'pp_comm_exposed_time', 'dp_comm_exposed_time', 
    'total_time', 'total_efficiency', 'sample_rate', 'throughput_per_proc'
]
# ----------------

def main():
    all_rows = []

    # 检查目录
    if not os.path.exists(SOURCE_DIRECTORY):
        print(f"错误：目录 '{SOURCE_DIRECTORY}' 不存在。")
        return

    # 获取所有 CSV 文件
    csv_files = [f for f in os.listdir(SOURCE_DIRECTORY) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print("未在目录下找到 CSV 文件。")
        return

    print(f"正在处理 {len(csv_files)} 个文件...")

    for filename in csv_files:
        file_path = os.path.join(SOURCE_DIRECTORY, filename)
        
        try:
            # 1. 读取传统 CSV（首行为表头）
            # 尝试不同编码以防止中文乱码
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            except:
                df = pd.read_csv(file_path, encoding='gbk')

            if df.empty:
                print(f"警告：文件 {filename} 为空，已跳过。")
                continue

            # 2. 提取数据
            # 创建一行数据，首位是文件名
            row_data = {'File_Name': filename}
            
            # 提取该文件第一行中对应的字段
            first_row = df.iloc[0]
            for field in TARGET_FIELDS:
                if field in df.columns:
                    val = first_row[field]
                    # 尝试转为数值，无法转换则保留原样
                    try:
                        row_data[field] = pd.to_numeric(val)
                    except (ValueError, TypeError):
                        row_data[field] = val
                else:
                    row_data[field] = None # 字段不存在则填空

            all_rows.append(row_data)

        except Exception as e:
            print(f"处理文件 {filename} 时发生错误: {e}")

    # 3. 汇总并导出
    if all_rows:
        result_df = pd.DataFrame(all_rows)
        
        # 确保列顺序：File_Name 在最前，随后是指定的关键字段
        # 使用 list(dict.fromkeys()) 去除你提供的列表中可能重复的字段名（如 num_procs）
        clean_fields = list(dict.fromkeys(TARGET_FIELDS))
        final_columns = ['File_Name'] + [f for f in clean_fields if f in result_df.columns]
        
        result_df = result_df[final_columns]

        # 导出为 Excel (xlsx) 避免乱码
        result_df.to_excel(OUTPUT_FILE, index=False)
        print(f"--- 任务完成 ---")
        print(f"汇总结果已保存至: {OUTPUT_FILE}")
    else:
        print("未能提取到任何有效数据。")

if __name__ == '__main__':
    main()
