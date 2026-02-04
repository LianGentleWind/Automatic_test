#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
共享工具模块
提供配置加载、日志设置等通用功能
"""

import os
import yaml
import logging

# ==================== 配置 ====================
# YAML配置文件路径（相对于运行目录）
CONFIG_YAML_PATH = "./automatic/config.yaml"

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
