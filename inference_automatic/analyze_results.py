#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结果分析脚本
功能：读取所有CSV结果文件，提取关键指标，生成汇总分析表格
"""

import os
import csv
import gzip
import yaml
from pathlib import Path

# 配置文件路径
CONFIG_YAML_PATH = "./automatic/config.yaml"


def load_config():
    """加载配置文件"""
    with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging():
    """设置日志"""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)


def parse_transposed_csv(csv_file, logger):
    """
    解析转置格式的CSV文件（支持gzip压缩）
    
    Args:
        csv_file: CSV文件路径
        logger: 日志对象
    
    Returns:
        dict: 解析后的数据，结构为 {category: {field_name: [values]}}
    """
    opener = gzip.open if csv_file.endswith('.gz') else open
    mode = 'rt' if csv_file.endswith('.gz') else 'r'
    
    data = {}
    
    try:
        with opener(csv_file, mode, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                category = row.get('category', '')
                field_name = row.get('field_name', '')
                
                if not category or not field_name:
                    continue
                
                if category not in data:
                    data[category] = {}
                
                values = []
                for key in sorted(row.keys()):
                    if key.startswith('run_'):
                        values.append(row[key])
                
                data[category][field_name] = values
        
        logger.info(f"成功解析CSV文件: {csv_file}, 类别数: {len(data)}")
    
    except Exception as e:
        logger.error(f"解析CSV文件失败: {csv_file}, 错误: {str(e)}")
        raise
    
    return data


def extract_param_value_from_filename(filename, param_name):
    """
    从配置文件名中提取参数值
    
    Args:
        filename: 文件名，如 "runtime_mem1_GiB_50.json"
        param_name: 参数名称，如 "mem1_GiB"
    
    Returns:
        str or None: 参数值
    """
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
    """
    sys_config = csv_data.get('system_resource_config', {})
    
    field_name = param_name.replace('_', '.')
    possible_names = [
        field_name,
        param_name,
        field_name.split('.')[-1],
    ]
    
    for name in possible_names:
        if name in sys_config:
            values = sys_config[name]
            if values and len(values) > 0:
                return values[0]
    
    return None


def analyze_results(results_dir, param_name, output_file, key_fields, logger):
    """
    分析所有结果并生成汇总表格
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
    
    all_results = []
    
    for csv_file in csv_files:
        try:
            csv_data = parse_transposed_csv(csv_file, logger)
            
            # 提取参数值
            filename = os.path.basename(csv_file)
            param_value = extract_param_value_from_filename(filename, param_name)
            
            if param_value is None:
                param_value = extract_param_from_csv_data(csv_data, param_name)
            
            if param_value is None:
                dir_name = os.path.basename(os.path.dirname(csv_file))
                param_value = extract_param_value_from_filename(dir_name, param_name)
            
            if param_value is None:
                logger.warning(f"无法从 {csv_file} 提取参数值，跳过")
                continue
            
            result_row = {
                'param_value': param_value,
                'csv_file': csv_file
            }
            
            for field in key_fields:
                value = None
                for category in ['performance_throughput', 'other_stats_metrics', 
                                'system_resource_config', 'parallel_strategy_config']:
                    if category in csv_data and field in csv_data[category]:
                        values = csv_data[category][field]
                        if values and len(values) > 0:
                            value = values[0]
                            break
                
                result_row[field] = value
            
            for category, fields in csv_data.items():
                for field_name, values in fields.items():
                    if field_name not in result_row and values and len(values) > 0:
                        result_row[f"{category}.{field_name}"] = values[0]
            
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
        all_results.sort(key=lambda x: str(x.get('param_value', '')))
    
    # 生成汇总表格
    logger.info(f"生成汇总表格: {output_file}")
    
    all_fields = set(['param_value', 'csv_file'])
    for result in all_results:
        all_fields.update(result.keys())
    
    output_fields = ['param_value', 'csv_file']
    output_fields.extend(key_fields)
    other_fields = sorted([f for f in all_fields if f not in output_fields])
    output_fields.extend(other_fields)
    
    output_dir_path = os.path.dirname(output_file) if os.path.dirname(output_file) else '.'
    os.makedirs(output_dir_path, exist_ok=True)
    
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


def main():
    """主函数"""
    conf = load_config()
    
    paths = conf.get('paths', {})
    ts = conf.get('test_setting', {})
    analysis = conf.get('analysis', {})
    
    # 获取参数名称
    param_name = ts.get('param_name', 'param')
    
    # 获取 runtime 名称用于组织输出
    runtime_prefix = os.path.splitext(os.path.basename(paths['base_runtime_config']))[0]
    
    # 构建搜索目录
    test_folder_name = f"{runtime_prefix}_{param_name}_scan"
    specific_results_dir = os.path.join(paths['output_root'], test_folder_name)
    
    # 构造输出文件路径
    base_output_dir = os.path.dirname(analysis.get('output_file', './automatic/analysis_summary.csv'))
    final_output_file = os.path.join(base_output_dir, f"analysis_summary_{runtime_prefix}_{param_name}.csv")

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
            key_fields=analysis.get('target_columns', []),
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
