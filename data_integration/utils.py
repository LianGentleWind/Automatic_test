"""
通用工具模块
Common utilities for data integration
"""

import os
import yaml
import pandas as pd
from datetime import datetime
from glob import glob


def load_config(config_path: str) -> dict:
    """
    加载并解析 YAML 配置文件
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        配置字典
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


def smart_read_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """
    智能读取 CSV 文件，自动检测编码
    
    Args:
        file_path: CSV 文件路径
        **kwargs: 传递给 pd.read_csv 的其他参数
    
    Returns:
        DataFrame
    """
    encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin1']
    
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # 如果所有编码都失败，使用默认编码并忽略错误
    return pd.read_csv(file_path, encoding='utf-8', errors='ignore', **kwargs)


def get_timestamp() -> str:
    """
    获取当前时间戳字符串
    
    Returns:
        格式化的时间戳 YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: str) -> None:
    """
    确保目录存在，不存在则创建
    
    Args:
        path: 目录路径
    """
    os.makedirs(path, exist_ok=True)


def find_files(source_dir: str, pattern: str) -> list:
    """
    查找匹配的文件
    
    Args:
        source_dir: 源目录
        pattern: 文件匹配模式（如 *.csv）
    
    Returns:
        匹配的文件路径列表
    """
    search_pattern = os.path.join(source_dir, pattern)
    return glob(search_pattern)


def apply_filters(df: pd.DataFrame, filters: list) -> pd.DataFrame:
    """
    根据过滤条件筛选数据
    
    Args:
        df: 原始 DataFrame
        filters: 过滤条件列表
    
    Returns:
        筛选后的 DataFrame
    """
    if not filters:
        return df
    
    result = df.copy()
    
    for f in filters:
        field = f.get('field')
        operator = f.get('operator', '==')
        values = f.get('values', [])
        
        if field not in result.columns:
            print(f"警告: 过滤字段 '{field}' 不存在，已跳过")
            continue
        
        if not values:
            continue
        
        value = values[0] if len(values) == 1 else values
        
        if operator == '==':
            result = result[result[field] == value]
        elif operator == '!=':
            result = result[result[field] != value]
        elif operator == '>':
            result = result[result[field] > value]
        elif operator == '<':
            result = result[result[field] < value]
        elif operator == '>=':
            result = result[result[field] >= value]
        elif operator == '<=':
            result = result[result[field] <= value]
        elif operator == 'in':
            result = result[result[field].isin(values)]
        elif operator == 'not_in':
            result = result[~result[field].isin(values)]
        else:
            print(f"警告: 未知操作符 '{operator}'，已跳过")
    
    return result


def transpose_wide_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    转置宽格式 CSV（首列为字段名，后续列为运行结果）
    
    原始格式:
        field_name, run_0, run_1, ...
        model_name, model_a, model_b, ...
    
    转置后:
        model_name, other_field, ...
        model_a, value_a, ...
        model_b, value_b, ...
    
    Args:
        df: 原始宽格式 DataFrame
    
    Returns:
        转置后的 DataFrame
    """
    # 设置第一列为索引，然后转置
    transposed = df.set_index(df.columns[0]).T
    
    # 重置索引
    transposed = transposed.reset_index(drop=True)
    
    return transposed


def format_output_filename(template: str) -> str:
    """
    格式化输出文件名，替换模板变量
    
    Args:
        template: 文件名模板
    
    Returns:
        格式化后的文件名
    """
    return template.replace("{timestamp}", get_timestamp())
