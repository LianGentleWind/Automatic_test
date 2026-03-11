#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结果分析脚本
功能：读取所有CSV结果文件，提取关键指标，生成汇总分析表格
支持：1~2 个扫描参数的结果提取
"""

import os
import csv
import gzip
from pathlib import Path

from common import (
    load_yaml_config, setup_logging, generate_param_name_from_path,
    parse_scan_params, get_scan_dimension, extract_param_values_from_combo_dir,
    PARAM_SEPARATOR, CONFIG_YAML_PATH
)

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
    从文件名或目录名中提取单个参数值
    
    Args:
        filename: 文件名，如 runtime_mem1_GiB_50.json
        param_name: 参数名称，如 mem1_GiB
    
    Returns:
        str or None: 参数值
    """
    # 移除扩展名
    name_without_ext = os.path.splitext(filename)[0]
    
    parts = name_without_ext.split('_')
    param_parts = param_name.split('_')
    param_len = len(param_parts)
    
    for i in range(len(parts) - param_len):
        if parts[i:i+param_len] == param_parts:
            if i + param_len < len(parts):
                return parts[i + param_len]
    
    return None

def extract_param_from_csv_data(csv_data, param_name):
    """
    从CSV数据中提取参数值
    
    Args:
        csv_data: 解析后的CSV数据
        param_name: 参数名称
    
    Returns:
        str or None: 参数值
    """
    field_name = param_name.replace('_', '.')
    
    possible_names = [
        field_name,
        param_name,
        field_name.split('.')[-1],
    ]
    
    for name in possible_names:
        if name in csv_data:
            values = csv_data[name]
            if values and len(values) > 0:
                return values[0]
    
    return None

def extract_multi_param_values(csv_file, param_names, logger):
    """
    从 CSV 文件路径中提取多个参数的值
    
    优先从目录名提取（使用 PARAM_SEPARATOR 分割），失败则逐个尝试从 CSV 数据中提取。
    
    Args:
        csv_file: CSV 文件完整路径
        param_names: 参数名称列表
        logger: 日志对象
    
    Returns:
        dict: {param_name: value_str}，提取失败的参数值为 None
    """
    result = {}
    dir_name = os.path.basename(os.path.dirname(csv_file))
    
    if len(param_names) == 1:
        # 单参数：使用原有逻辑
        pn = param_names[0]
        val = extract_param_value_from_filename(dir_name, pn)
        if val is None:
            val = extract_param_value_from_filename(os.path.basename(csv_file), pn)
        result[pn] = val
    else:
        # 多参数：从组合目录名中提取
        result = extract_param_values_from_combo_dir(dir_name, param_names)
        
        # 检查是否都提取成功
        for pn in param_names:
            if pn not in result or result[pn] is None:
                logger.warning(f"  无法从目录名 '{dir_name}' 提取参数 '{pn}'")
    
    return result

# ==================== 结果分析 ====================
def analyze_results(results_dir, param_names, output_file, key_fields, logger):
    """
    分析所有结果并生成汇总表格（支持多参数）
    
    Args:
        results_dir: 结果目录
        param_names: 参数名称列表（1 或 2 个）
        output_file: 输出文件路径
        key_fields: 需要提取的关键字段列表
        logger: 日志对象
    """
    logger.info(f"开始分析结果，目录: {results_dir}")
    logger.info(f"参数: {', '.join(param_names)}")
    
    # 扫描所有CSV文件
    csv_files = []

    TARGET_FILES = [
        "pd-split-request-optimal_result_best.csv",
        "pd-split-request-optimal_decoder_best.csv",
        "pd-split-request-optimal_prefill_best.csv"
    ]

    for root, dirs, files in os.walk(results_dir):
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
            param_values = extract_multi_param_values(csv_file, param_names, logger)
            
            # 检查是否所有参数都已提取
            missing = [pn for pn in param_names if param_values.get(pn) is None]
            if missing:
                # 尝试从 CSV 数据中补充
                for pn in missing:
                    val = extract_param_from_csv_data(csv_data, pn)
                    if val is not None:
                        param_values[pn] = val
                
                # 再次检查
                still_missing = [pn for pn in param_names if param_values.get(pn) is None]
                if still_missing:
                    logger.warning(f"无法从 {csv_file} 提取参数 {still_missing}，跳过")
                    continue
            
            # 构造结果行
            result_row = {'csv_file': csv_file}
            
            # 单参数时保持 param_value 列兼容
            if len(param_names) == 1:
                result_row['param_value'] = param_values[param_names[0]]
            
            # 多参数时每个参数一列
            for pn in param_names:
                result_row[pn] = param_values[pn]
            
            # 提取关键字段
            for field in key_fields:
                if field in csv_data:
                    values = csv_data[field]
                    if values and len(values) > 0:
                        result_row[field] = values[0]
                else:
                    result_row[field] = None
            
            # 提取所有其他字段
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
    
    # 按参数值排序（多参数时按 param1 -> param2 排序）
    def sort_key(row):
        keys = []
        for pn in param_names:
            v = row.get(pn, '')
            try:
                keys.append(float(v))
            except (ValueError, TypeError):
                keys.append(str(v))
        return keys
    
    try:
        all_results.sort(key=sort_key)
    except TypeError:
        # 混合类型无法比较，按字符串排序
        all_results.sort(key=lambda x: [str(x.get(pn, '')) for pn in param_names])
    
    # 生成汇总表格
    logger.info(f"生成汇总表格: {output_file}")
    
    # 确定输出字段顺序
    all_fields = set()
    for result in all_results:
        all_fields.update(result.keys())
    
    # 输出字段顺序：param 列 -> csv_file -> key_fields -> 其他
    output_fields = []
    if len(param_names) == 1:
        output_fields.append('param_value')
    for pn in param_names:
        if pn not in output_fields:
            output_fields.append(pn)
    output_fields.append('csv_file')
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
    logger.info(f"  参数名称: {', '.join(param_names)}")
    logger.info(f"  扫描维度: {len(param_names)}")
    logger.info(f"  关键字段: {', '.join(key_fields)}")
    logger.info("=" * 60)

# ==================== 主函数 ====================
def main():
    """主函数"""
    # 加载配置
    config = load_yaml_config(CONFIG_YAML_PATH)
    
    scan_config = config.get('scan', {})
    analyze_config = config.get('analyze', {})
    
    # 解析扫描参数（兼容新旧格式）
    scan_params = parse_scan_params(scan_config)
    param_names = [generate_param_name_from_path(sp['param_path']) for sp in scan_params]
    
    # 获取 runtime 名称用于组织输出
    runtime_name = Path(scan_config['base_runtime_config']).stem
    
    # 自动调整搜索路径到对应的子目录
    specific_results_dir = os.path.join(analyze_config['results_dir'], runtime_name)
    
    # 构造输出文件路径
    base_output_file = analyze_config['output_file']
    output_dir_path = os.path.dirname(base_output_file) if os.path.dirname(base_output_file) else '.'
    final_output_file = os.path.join(output_dir_path, f"analysis_summary_{runtime_name}.csv")

    # 设置日志
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("开始结果分析")
    logger.info(f"扫描维度: {len(param_names)}")
    logger.info(f"参数: {', '.join(param_names)}")
    logger.info("=" * 60)
    
    try:
        analyze_results(
            results_dir=specific_results_dir,
            param_names=param_names,
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
