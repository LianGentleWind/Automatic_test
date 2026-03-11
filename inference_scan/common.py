#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
共享工具模块
提供配置加载、日志设置、多参数命名等通用功能
"""

import os
import yaml
import logging
from itertools import product

# ==================== 配置 ====================
# YAML配置文件路径（相对于运行目录）
CONFIG_YAML_PATH = "./automatic/config.yaml"

# 多参数目录/名称分隔符（双下划线）
PARAM_SEPARATOR = "__"

# ==================== 日志设置 ====================
def setup_logging(log_file=None):
    """
    设置日志
    
    Args:
        log_file: 可选的日志文件路径。如果提供，同时输出到文件和控制台；否则仅输出到控制台。
    
    Returns:
        logger: 配置好的日志对象
    """
    handlers = [logging.StreamHandler()]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )
    return logging.getLogger(__name__)

# ==================== YAML配置加载 ====================
def load_yaml_config(config_file=None):
    """
    加载YAML配置文件
    
    Args:
        config_file: 配置文件路径，默认使用 CONFIG_YAML_PATH
    
    Returns:
        dict: 解析后的配置字典
    """
    if config_file is None:
        config_file = CONFIG_YAML_PATH
        
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config

# ==================== 参数名称工具 ====================
def generate_param_name_from_path(param_path):
    """
    从参数路径生成参数名称（用于文件名）
    
    将 "mem1.GiB" 转换为 "mem1_GiB"
    将 "networks[0].bandwidth" 转换为 "networks_0_bandwidth"
    
    Args:
        param_path: 参数路径字符串
    
    Returns:
        str: 适合用于文件名的参数名称
    """
    name = param_path.replace('.', '_').replace('[', '_').replace(']', '')
    return name

def format_value_for_filename(value):
    """
    将参数值转换为适合文件名的格式
    
    Args:
        value: 参数值（可能是数值、字符串或列表）
    
    Returns:
        str: 适合用于文件名的字符串
    
    Examples:
        1 -> "1"
        [1, 1] -> "1_1"
        "[1,1]" -> "1_1"
        [None, 50] -> "50"
    """
    if isinstance(value, list):
        # Python list 对象，过滤掉 None
        valid_parts = [str(v) for v in value if v is not None]
        return '_'.join(valid_parts)
    elif isinstance(value, str) and value.startswith('['):
        # 字符串格式的列表，如 "[1,1]" 或 "[null, 50]"
        inner = value.strip('[]')
        parts = [p.strip() for p in inner.split(',')]
        # 过滤掉 null/None 字符串
        valid_parts = [p for p in parts if p.lower() != 'null' and p != 'None']
        return '_'.join(valid_parts)
    else:
        return str(value)

def parse_csv_value(value_str):
    """
    解析 CSV 中的值字符串
    
    Args:
        value_str: CSV 中读取的字符串值
    
    Returns:
        解析后的值（int、float、list 或原始字符串）
    
    Examples:
        "1" -> 1
        "1.5" -> 1.5
        "[1,1]" -> [1, 1]
        "[null, 50]" -> [None, 50]
    """
    value_str = value_str.strip()
    
    # 检查是否为列表格式
    if value_str.startswith('[') and value_str.endswith(']'):
        inner = value_str[1:-1]
        parts = [p.strip() for p in inner.split(',')]
        # 尝试转换为数值
        result = []
        for p in parts:
            if p.lower() == 'null':
                result.append(None)
                continue
                
            try:
                result.append(int(p))
            except ValueError:
                try:
                    result.append(float(p))
                except ValueError:
                    result.append(p)
        return result
    
    # 尝试转换为整数
    try:
        return int(value_str)
    except ValueError:
        pass
    
    # 尝试转换为浮点数
    try:
        return float(value_str)
    except ValueError:
        pass
    
    # 保持原始字符串
    return value_str

# ==================== 多参数扫描工具 ====================
def parse_scan_params(scan_config):
    """
    统一解析扫描参数配置，兼容新旧两种格式
    
    新格式（scan_params 列表）:
        scan_params:
          - param_path: "mem1.GiB"
            param_mode: {...}
          - param_path: "networks[0].bandwidth"
            param_mode: {...}
    
    旧格式（单一 param_path + param_mode）:
        param_path: "mem1.GiB"
        param_mode: {...}
    
    Args:
        scan_config: scan 配置字典
    
    Returns:
        list[dict]: 参数配置列表，每个元素包含 'param_path' 和 'param_mode'
    """
    # 新格式：scan_params 列表
    if 'scan_params' in scan_config:
        return scan_config['scan_params']
    
    # 旧格式：单一 param_path + param_mode，适配为列表
    if 'param_path' in scan_config:
        return [{
            'param_path': scan_config['param_path'],
            'param_mode': scan_config.get('param_mode', {})
        }]
    
    raise ValueError("配置中未找到 scan_params 或 param_path")

def get_scan_dimension(scan_config):
    """
    获取扫描维度数
    
    Args:
        scan_config: scan 配置字典
    
    Returns:
        int: 扫描参数个数（1, 2, ...）
    """
    return len(parse_scan_params(scan_config))

def build_combo_dir_name(param_names, values):
    """
    生成组合参数的目录名
    
    单参数: config_mem1_GiB_50
    双参数: config_mem1_GiB_50__networks_0_bandwidth_100
    
    Args:
        param_names: 参数名称列表 ["mem1_GiB", "networks_0_bandwidth"]
        values: 对应参数值列表 [50, 100]
    
    Returns:
        str: 目录名
    """
    parts = []
    for name, val in zip(param_names, values):
        value_str = format_value_for_filename(val)
        parts.append(f"{name}_{value_str}")
    return "config_" + PARAM_SEPARATOR.join(parts)

def build_combo_sys_name(original_name, param_names, values):
    """
    生成组合参数的系统名称
    
    单参数: SOW_S3_POR_mem1_GiB_50
    双参数: SOW_S3_POR_mem1_GiB_50__networks_0_bandwidth_100
    
    Args:
        original_name: 原始系统名称
        param_names: 参数名称列表
        values: 对应参数值列表
    
    Returns:
        str: 系统名称
    """
    parts = []
    for name, val in zip(param_names, values):
        value_str = format_value_for_filename(val)
        parts.append(f"{name}_{value_str}")
    suffix = PARAM_SEPARATOR.join(parts)
    return f"{original_name}_{suffix}"

def build_combo_filename(prefix, param_names, values, ext=".json"):
    """
    生成组合参数的文件名
    
    Args:
        prefix: 文件名前缀（如 "runtime", "sys"）
        param_names: 参数名称列表
        values: 对应参数值列表
        ext: 文件扩展名
    
    Returns:
        str: 文件名
    """
    parts = []
    for name, val in zip(param_names, values):
        value_str = format_value_for_filename(val)
        parts.append(f"{name}_{value_str}")
    suffix = PARAM_SEPARATOR.join(parts)
    return f"{prefix}_{suffix}{ext}"

def generate_param_combinations(all_param_values):
    """
    对多个参数的值列表做笛卡尔积
    
    Args:
        all_param_values: 二维列表，如 [[1,2,3], [10,20]]
    
    Returns:
        list[tuple]: 所有组合，如 [(1,10), (1,20), (2,10), ...]
    """
    return list(product(*all_param_values))

def extract_param_values_from_combo_dir(dir_name, param_names):
    """
    从组合目录名中提取各参数的值
    
    Args:
        dir_name: 目录名，如 "config_mem1_GiB_50__networks_0_bandwidth_100"
        param_names: 参数名称列表 ["mem1_GiB", "networks_0_bandwidth"]
    
    Returns:
        dict: {param_name: value_str}，如 {"mem1_GiB": "50", "networks_0_bandwidth": "100"}
    """
    result = {}
    
    # 去掉 "config_" 前缀
    content = dir_name
    if content.startswith("config_"):
        content = content[len("config_"):]
    
    # 按 PARAM_SEPARATOR 分割各参数段
    segments = content.split(PARAM_SEPARATOR)
    
    for i, param_name in enumerate(param_names):
        if i < len(segments):
            segment = segments[i]
            param_parts = param_name.split('_')
            param_len = len(param_parts)
            
            # 在 segment 中查找 param_name 前缀并提取后面的值
            seg_parts = segment.split('_')
            if len(seg_parts) > param_len and seg_parts[:param_len] == param_parts:
                value_str = '_'.join(seg_parts[param_len:])
                result[param_name] = value_str
            else:
                # 回退：segment 整体作为值（兼容简单情况）
                result[param_name] = segment
    
    return result

def is_sys_config_param(param_path):
    """
    判断参数是否属于系统配置（而非 runtime 配置）
    
    Args:
        param_path: 参数路径字符串
    
    Returns:
        bool
    """
    sys_prefixes = ('sys_list', 'networks', 'mem', 'matrix', 'vector')
    return param_path.startswith(sys_prefixes)
