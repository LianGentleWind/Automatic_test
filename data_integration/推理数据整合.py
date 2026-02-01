import pandas as pd
import numpy as np

# --- 配置区域 ---
INPUT_FILE = './raw_data/inference/inference_DV1024P_Rubin256P_S3_2048P_U1_1920P.csv'
OUTPUT_FILE = './data/inference/inference_DV1024P_Rubin256P_S3_2048P_U1_1920P.xlsx'
# ----------------

def main():
    try:
        # 1. 读取并转置数据
        try:
            raw_df = pd.read_csv(INPUT_FILE, header=None, encoding='utf-8-sig')
        except:
            raw_df = pd.read_csv(INPUT_FILE, header=None, encoding='gbk')
        
        df = raw_df.set_index(0).T
        
        # 2. 定义关键字段名（请确保与 CSV 中完全一致）
        iv_model = 'model_name'
        iv_system = 'prefill_system_name'
        iv_time = 'decoder_time_limit(ms)'
        iv_npu_num = 'decoder_num_npu'
        
        dv_decode_total = 'decoder_throughput(token/s)'
        dv_decode_per = 'decoder_throughput_per_npu(token/s)'
        dv_prefill_total = 'prefill_throughput(token/s)'
        dv_prefill_per = 'prefill_throughput_per_npu(token/s)'

        # 3. 强制数值转换
        numeric_cols = [iv_time, iv_npu_num, dv_decode_total, dv_decode_per, dv_prefill_total, dv_prefill_per]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. 构建两行结构：整机行 (Total) 与 单卡行 (Single)
        # --- 创建整机部分数据 ---
        df_total = df.copy()
        df_total['row_label'] = df_total[iv_npu_num].astype(str) # 使用 NPU 数量作为标签
        df_total['decode_val'] = df_total[dv_decode_total]      # 填入整机吞吐
        df_total['prefill_val'] = df_total[dv_prefill_total]    # 填入整机 Prefill
        df_total['row_sort'] = 0                                # 排序辅助

        # --- 创建单卡部分数据 ---
        df_single = df.copy()
        df_single['row_label'] = '单卡'
        df_single['decode_val'] = df_single[dv_decode_per]      # 填入单卡吞吐
        df_single['prefill_val'] = df_single[dv_prefill_per]    # 填入单卡 Prefill
        df_single['row_sort'] = 1                               # 排序辅助

        # 合并所有行
        combined = pd.concat([df_total, df_single])

        # 5. 透视表生成函数
        def create_pivot_block(value_col, prefix):
            # 以 model_name 为首级索引实现片区划分
            pivot = combined.pivot_table(
                index=[iv_model, iv_system, 'row_sort', 'row_label'],
                columns=iv_time,
                values=value_col,
                aggfunc='first'
            )
            
            # 横轴排序：从高到低降序
            sorted_cols = sorted(pivot.columns, reverse=True)
            pivot = pivot.reindex(columns=sorted_cols)
            
            # 数值处理：取整
            pivot = pivot.round(0)
            
            # 列名美化
            pivot.columns = [f'{prefix}_{int(c)}ms' for c in pivot.columns]
            return pivot

        # 6. 生成 Decode 和 Prefill 两个数据块
        pivot_decode = create_pivot_block('decode_val', 'Decode')
        pivot_prefill = create_pivot_block('prefill_val', 'Prefill')

        # 7. 水平合并并清理索引
        final_table = pd.concat([pivot_decode, pivot_prefill], axis=1)
        
        # 移除辅助排序位，重命名索引，使表格外观专业
        final_table = final_table.reset_index(level='row_sort', drop=True)
        final_table.index.names = ['Model', 'System', 'Metric']

        # 8. 导出到 Excel
        # 使用 ExcelWriter 确保样式和多模型片区能清晰展现
        final_table.to_excel(OUTPUT_FILE)
        
        print(f"处理完成！")
        print(f"1. 模型片区：已按 {iv_model} 自动切分")
        print(f"2. 数据对应：整机行填充 Total 值，单卡行填充 Per NPU 值")
        print(f"3. 排序：横轴 Time Limit 已按降序排列")
        print(f"结果文件: {OUTPUT_FILE}")

    except Exception as e:
        print(f"程序运行发生错误: {e}")

if __name__ == '__main__':
    main()
