#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结果分析脚本
功能：读取所有CSV结果文件，提取关键指标，生成汇总分析表格
"""

import os
import csv
import gzip
from pathlib import Path

from common import load_yaml_config, setup_logging, generate_param_name_from_path, CONFIG_YAML_PATH

# ==================== CSV解析 ====================
def parse_transposed_csv(csv_file, logger):
    """
    解析转置格式的CSV文件（支持gzip压缩）
    
    Args:
        csv_file: CSV文件路径
        logger: 日志对象
    
    Returns:
        dict: 解析后的数据，结构为 {field_name: [values]}
    """
    # 支持gzip压缩
    opener = gzip.open if csv_file.endswith('.gz') else open
    mode = 'rt' if csv_file.endswith('.gz') else 'r'
    
    data = {}
    
    try:
        with opener(csv_file, mode, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                field_name = row.get('field_name', '')
                
                if not field_name:
                    continue
                
                # 提取所有run的值
                values = []
                for key in sorted(row.keys()):
                    if key.startswith('run_'):
                        values.append(row[key])
                
                data[field_name] = values
        
        logger.info(f"成功解析CSV文件: {csv_file}, 字段数: {len(data)}")
    
    except Exception as e:
        logger.error(f"解析CSV文件失败: {csv_file}, 错误: {str(e)}")
        raise
    
    return data

# ==================== 参数值提取 ====================
def extract_param_value_from_filename(filename, param_name):
    """
    从配置文件名中提取参数值
    
    Args:
        filename: 文件名，如 runtime_mem1_GiB_50.json 或对应的CSV文件名
        param_name: 参数名称，如 mem1_GiB
    
    Returns:
        str or None: 参数值，如 50
    """
    # 移除扩展名
    name_without_ext = os.path.splitext(filename)[0]
    
    # 查找参数名称和值
    # 格式: {prefix}_{param_name}_{value}
    parts = name_without_ext.split('_')
    
    # 查找param_name的位置
    param_parts = param_name.split('_')
    param_len = len(param_parts)
    
    for i in range(len(parts) - param_len):
        if parts[i:i+param_len] == param_parts:
            # 找到参数名称，下一个应该是值
            if i + param_len < len(parts):
                return parts[i + param_len]
    
    return None

def extract_param_from_csv_data(csv_data, param_name):
    """
    从CSV数据中提取参数值
    
    Args:
        csv_data: 解析后的CSV数据
        param_name: 参数名称，如 mem1_GiB
    
    Returns:
        str or None: 参数值
    """
    # 将param_name转换为可能的字段名
    # mem1_GiB -> mem1.GiB 或 GiB
    field_name = param_name.replace('_', '.')
    
    # 尝试多种可能的字段名
    possible_names = [
        field_name,
        param_name,
        field_name.split('.')[-1],  # 只取最后一部分
    ]
    
    for name in possible_names:
        if name in csv_data:
            values = csv_data[name]
            if values and len(values) > 0:
                return values[0]  # 返回第一个值
    
    return None

# ==================== 结果分析 ====================
def analyze_results(results_dir, param_name, output_file, key_fields, logger):
    """
    分析所有结果并生成汇总表格
    
    Args:
        results_dir: 结果目录
        param_name: 参数名称（用于从文件名提取）
        output_file: 输出文件路径
        key_fields: 需要提取的关键字段列表
        logger: 日志对象
    """
    logger.info(f"开始分析结果，目录: {results_dir}")
    
    # 扫描所有CSV文件
    csv_files = []

    # 目标文件优先级列表
    TARGET_FILES = [
        "pd-split-request-optimal_result_best.csv",
        "pd-split-request-optimal_decoder_best.csv",
        "pd-split-request-optimal_prefill_best.csv"
    ]

    for root, dirs, files in os.walk(results_dir):
        # 按优先级顺序查找目标文件
        target_found = None
        for target in TARGET_FILES:
            if target in files:
                target_found = os.path.join(root, target)
                break
            elif f"{target}.gz" in files:
                target_found = os.path.join(root, f"{target}.gz")
                break
        if target_found:
            csv_files.append(target_found)
        
    if not csv_files:
        logger.warning(f"在 {results_dir} 下未找到任何目标CSV文件")
        return
    
    logger.info(f"找到 {len(csv_files)} 个CSV文件")
    
    # 解析所有CSV文件
    all_results = []
    
    for csv_file in csv_files:
        try:
            csv_data = parse_transposed_csv(csv_file, logger)
            
            # 提取参数值
            # 首先尝试从文件名提取
            filename = os.path.basename(csv_file)
            param_value = extract_param_value_from_filename(filename, param_name)
            
            # 如果从文件名提取失败，尝试从CSV数据中提取
            if param_value is None:
                param_value = extract_param_from_csv_data(csv_data, param_name)
            
            # 如果还是提取不到，尝试从目录名提取
            if param_value is None:
                dir_name = os.path.basename(os.path.dirname(csv_file))
                param_value = extract_param_value_from_filename(dir_name, param_name)
            
            if param_value is None:
                logger.warning(f"无法从 {csv_file} 提取参数值，跳过")
                continue
            
            # 提取关键字段的值
            result_row = {
                'param_value': param_value,
                'csv_file': csv_file
            }
            
            # 提取所有关键字段
            for field in key_fields:
                if field in csv_data:
                    values = csv_data[field]
                    if values and len(values) > 0:
                        result_row[field] = values[0]
                else:
                    result_row[field] = None
            
            # 提取所有其他字段（用于完整记录）
            for field_name, values in csv_data.items():
                if field_name not in result_row and values and len(values) > 0:
                    result_row[field_name] = values[0]
            
            all_results.append(result_row)
        
        except Exception as e:
            logger.error(f"处理CSV文件失败: {csv_file}, 错误: {str(e)}")
            continue
    
    if not all_results:
        logger.warning("未提取到任何有效结果")
        return
    
    # 按参数值排序
    try:
        all_results.sort(key=lambda x: float(x['param_value']))
    except (ValueError, KeyError):
        # 如果无法转换为数字，按字符串排序
        all_results.sort(key=lambda x: str(x.get('param_value', '')))
    
    # 生成汇总表格
    logger.info(f"生成汇总表格: {output_file}")
    
    # 收集所有可能的字段名
    all_fields = set(['param_value', 'csv_file'])
    for result in all_results:
        all_fields.update(result.keys())
    
    # 确定输出字段顺序：param_value, csv_file, key_fields, 其他字段
    output_fields = ['param_value', 'csv_file']
    output_fields.extend(key_fields)
    other_fields = sorted([f for f in all_fields if f not in output_fields])
    output_fields.extend(other_fields)
    
    # 确保输出目录存在
    output_dir_path = os.path.dirname(output_file) if os.path.dirname(output_file) else '.'
    os.makedirs(output_dir_path, exist_ok=True)
    
    # 写入汇总 CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(all_results)
    
    logger.info(f"汇总表格已生成: {output_file}, 共 {len(all_results)} 条记录")
    
    # 打印摘要
    logger.info("=" * 60)
    logger.info("分析摘要:")
    logger.info(f"  处理的CSV文件数: {len(csv_files)}")
    logger.info(f"  有效结果数: {len(all_results)}")
    logger.info(f"  参数名称: {param_name}")
    logger.info(f"  关键字段: {', '.join(key_fields)}")
    logger.info("=" * 60)

# ==================== 主函数 ====================
def main():
    """主函数"""
    # 加载配置
    config = load_yaml_config(CONFIG_YAML_PATH)
    
    scan_config = config.get('scan', {})
    analyze_config = config.get('analyze', {})
    
    # 从 scan.param_path 派生参数名称（统一使用同一个配置源）
    param_path = scan_config['param_path']
    param_name = generate_param_name_from_path(param_path)
    
    # 获取 runtime 名称用于组织输出
    runtime_name = Path(scan_config['base_runtime_config']).stem
    
    # 自动调整搜索路径到对应的子目录
    specific_results_dir = os.path.join(analyze_config['results_dir'], runtime_name)
    
    # 构造输出文件路径（包含 runtime 名称）
    base_output_file = analyze_config['output_file']
    output_dir_path = os.path.dirname(base_output_file) if os.path.dirname(base_output_file) else '.'
    final_output_file = os.path.join(output_dir_path, f"analysis_summary_{runtime_name}.csv")

    # 设置日志
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("开始结果分析")
    logger.info("=" * 60)
    
    try:
        analyze_results(
            results_dir=specific_results_dir,
            param_name=param_name,
            output_file=final_output_file,
            key_fields=analyze_config.get('key_fields', []),
            logger=logger
        )
        
        logger.info("=" * 60)
        logger.info("结果分析完成")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
