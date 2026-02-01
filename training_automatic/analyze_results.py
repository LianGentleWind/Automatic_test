import pandas as pd
import glob
import os
import yaml
import re

def merge_arrange_results():
    try:
        with open("./automatic/config.yaml", "r") as f:
            conf = yaml.safe_load(f)
    except FileNotFoundError:
        print("错误：未找到 config.yaml。")
        return

    paths = conf.get('paths', {})
    ts = conf.get('test_setting', {})
    
    runtime_prefix = os.path.splitext(os.path.basename(paths['base_runtime']))[0]
    param_col_name = ts.get('param_name', 'param_value')
    output_root = paths.get('output_root', 'test/260115sow/output')
    final_summary_path = f"summary_{runtime_prefix}_{param_col_name}.csv"

    # 依然搜索带有 arrange 后缀的文件，因为这是后续脚本生成的
    test_folder_name = f"{runtime_prefix}_{param_col_name}_scan"
    search_pattern = os.path.join(output_root, test_folder_name, "*arrange.csv")
    target_files = glob.glob(search_pattern)

    if not target_files:
        print(f"未在 {test_folder_name} 下找到匹配的 *arrange.csv 文件。")
        return

    all_rows = []
    print(f"正在分析前缀为 {runtime_prefix} 的结果...")

    for file_path in target_files:
        try:
            df = pd.read_csv(file_path, nrows=1)
            if not df.empty:
                filename = os.path.basename(file_path)
                
                # 正则解析：匹配 参数名_数值_ (数值可能包含点或科学计数法)
                # 这样可以无视后面的 arrange.csv 后缀
                pattern = rf"_{param_col_name}_([\d\.]+)_"
                match = re.search(pattern, filename)
                
                if match:
                    test_val = match.group(1)
                else:
                    # 备选提取：res_runtime_param_val_arrange.csv -> 倒数第二个是 val
                    test_val = filename.split('_')[-2]

                df.insert(0, param_col_name, test_val)
                all_rows.append(df)
        except Exception as e:
            print(f"处理 {filename} 失败: {e}")

    if all_rows:
        final_df = pd.concat(all_rows, ignore_index=True)

        # 核心修复：将排序列强制转为数字，避免 str 和 float 混合排序报错
        final_df[param_col_name] = pd.to_numeric(final_df[param_col_name], errors='coerce')
        
        # 排序并保存
        final_df = final_df.sort_values(by=param_col_name, ascending=True)
        final_df.to_csv(final_summary_path, index=False)
        
        print("-" * 30)
        print(f"汇总完成！")
        print(f"列名: {param_col_name}")
        print(f"结果文件: {final_summary_path}")
        print("-" * 30)
    else:
        print("未提取到有效数据。")

if __name__ == "__main__":
    merge_arrange_results()
