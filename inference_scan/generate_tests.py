#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推理测试生成脚本
功能：从 config.yaml 读取配置，生成多个 runtime 配置文件和 run_simulations.sh 脚本
"""

import os
import json
import copy
from pathlib import Path

from common import load_yaml_config, generate_param_name_from_path, format_value_for_filename, CONFIG_YAML_PATH

# ==================== 参数值生成 ====================
def generate_arithmetic_values(start, end, step):
    """生成等差数列"""
    values = []
    current = start
    while current <= end:
        values.append(current)
        current += step
    return values

def generate_power_of_2_values(start_power, end_power):
    """生成2的幂次数列"""
    return [2**i for i in range(start_power, end_power + 1)]

def get_test_values(param_mode):
    """根据配置生成测试值列表"""
    value_type = param_mode.get('value_type', 'arithmetic')
    format_type = param_mode.get('format', 'single')
    
    if value_type == 'arithmetic':
        start = param_mode.get('start', 1)
        end = param_mode.get('end', 10)
        step = param_mode.get('step', 1)
        raw_values = generate_arithmetic_values(start, end, step)
        print(f"等差数列: start={start}, end={end}, step={step}")
    elif value_type == 'power_of_2':
        start_power = param_mode.get('start_power', 0)
        end_power = param_mode.get('end_power', 10)
        raw_values = generate_power_of_2_values(start_power, end_power)
        print(f"2的幂次: 2^{start_power} 到 2^{end_power}")
    else:
        raise ValueError(f"不支持的数值类型: {value_type}")
    
    # 格式化值
    formatted_values = []
    for v in raw_values:
        if format_type == 'single':
            formatted_values.append(v)
        elif format_type == 'pair':
            formatted_values.append([v, v])
        elif format_type == 'pair_null_first':
            formatted_values.append([None, v])
        else:
            raise ValueError(f"不支持的格式类型: {format_type}")
    
    return formatted_values

# ==================== 参数路径解析 ====================
def parse_param_path(config, param_path):
    """
    解析参数路径，返回最后一级的key和父对象
    
    Args:
        config: 配置字典
        param_path: 参数路径，如 'mem1.GiB' 或 'networks[0].bandwidth'
    
    Returns:
        (last_key, parent_obj): 最后一级的key和父对象
    """
    parts = param_path.split('.')
    current = config
    
    for i, part in enumerate(parts[:-1]):
        if '[' in part:
            key, index_str = part.split('[')
            index = int(index_str.rstrip(']'))
            if key not in current:
                raise KeyError(f"路径不存在: {'.'.join(parts[:i+1])}")
            current = current[key][index]
        else:
            if part not in current:
                raise KeyError(f"路径不存在: {'.'.join(parts[:i+1])}")
            current = current[part]
    
    return parts[-1], current

def set_param_value(config, param_path, value):
    """设置参数值"""
    last_key, parent = parse_param_path(config, param_path)
    
    # 处理包含 None 的列表（部分更新模式）
    if isinstance(value, list) and None in value:
        current_val = parent.get(last_key)
        if isinstance(current_val, list) and len(current_val) == len(value):
            merged_value = []
            for new_v, old_v in zip(value, current_val):
                merged_value.append(new_v if new_v is not None else old_v)
            parent[last_key] = merged_value
            return

    parent[last_key] = value

# ==================== 主生成逻辑 ====================
def generate():
    """主生成函数"""
    # 加载配置
    config = load_yaml_config(CONFIG_YAML_PATH)
    scan_config = config.get('scan', {})
    run_config = config.get('run', {})
    
    # 获取配置路径
    base_runtime_path = scan_config['base_runtime_config']
    base_sys_config_path = scan_config['base_sys_config']
    param_path = scan_config['param_path']
    output_dir = scan_config['output_dir']
    generated_configs_dir = scan_config['generated_configs_dir']
    commands_file = scan_config.get('commands_file', './automatic/run_simulations.sh')
    command_template = run_config.get('command', 'python3 run.py -c')
    
    # 生成参数名称
    param_name = generate_param_name_from_path(param_path)
    runtime_prefix = Path(base_runtime_path).stem
    
    print(f"基础 Runtime: {base_runtime_path}")
    print(f"参数路径: {param_path} -> {param_name}")
    
    # 读取基础配置
    with open(base_runtime_path, 'r', encoding='utf-8') as f:
        base_runtime = json.load(f)
    
    with open(base_sys_config_path, 'r', encoding='utf-8') as f:
        base_sys_config = json.load(f)
    
    original_sys_name = base_sys_config.get('name', 'System')
    
    # 创建目录
    config_subdir = os.path.join(generated_configs_dir, runtime_prefix)
    os.makedirs(config_subdir, exist_ok=True)
    
    results_base_dir = os.path.join(output_dir, runtime_prefix)
    os.makedirs(results_base_dir, exist_ok=True)
    
    # 生成测试值
    param_mode = scan_config.get('param_mode', {})
    test_values = get_test_values(param_mode)
    print(f"生成 {len(test_values)} 个参数值")
    
    # 判断参数是否在系统配置中
    is_sys_config_param = (
        param_path.startswith('sys_list') or
        param_path.startswith('networks') or
        param_path.startswith('mem') or
        param_path.startswith('matrix') or
        param_path.startswith('vector')
    )
    
    commands = []
    
    for val in test_values:
        new_runtime = copy.deepcopy(base_runtime)
        value_str = format_value_for_filename(val)
        
        # 创建独立输出目录
        sub_dir_name = f"config_{param_name}_{value_str}"
        specific_output_path = os.path.join(results_base_dir, sub_dir_name)
        os.makedirs(specific_output_path, exist_ok=True)
        
        # 更新 runtime 中的 output 字段
        for deploy_mode in new_runtime.keys():
            if isinstance(new_runtime[deploy_mode], dict) and \
               deploy_mode in ['pd-split-request-optimal', 'pd-fusion']:
                new_runtime[deploy_mode]['output'] = specific_output_path
        
        if is_sys_config_param:
            # 参数在系统配置中
            new_sys_config = copy.deepcopy(base_sys_config)
            
            try:
                set_param_value(new_sys_config, param_path, val)
                new_sys_config['name'] = f"{original_sys_name}_{param_name}_{value_str}"
                
                # 保存新的系统配置
                sys_filename = f"sys_{param_name}_{value_str}_{os.path.basename(base_sys_config_path)}"
                new_sys_path = os.path.join(config_subdir, sys_filename)
                with open(new_sys_path, 'w', encoding='utf-8') as f:
                    json.dump(new_sys_config, f, indent=2, ensure_ascii=False)
                
                # 更新 runtime 中的 sys_list 引用
                for deploy_mode in new_runtime.keys():
                    if deploy_mode in ['pd-split-request-optimal', 'pd-fusion']:
                        sys_list = new_runtime[deploy_mode].get('sys_list', [])
                        if isinstance(sys_list, list):
                            for i, sys_item in enumerate(sys_list):
                                if isinstance(sys_item, list):
                                    for j, sys_path in enumerate(sys_item):
                                        if sys_path == base_sys_config_path:
                                            sys_list[i][j] = new_sys_path
                                elif sys_item == base_sys_config_path:
                                    sys_list[i] = new_sys_path
                
            except (KeyError, TypeError, IndexError) as e:
                print(f"警告: 无法在系统配置中设置参数 {param_path}: {e}")
        else:
            # 参数在 runtime 配置中
            try:
                set_param_value(new_runtime, param_path, val)
            except (KeyError, TypeError, IndexError) as e:
                print(f"错误: 无法在 runtime 配置中设置参数 {param_path}: {e}")
                raise
        
        # 保存新的 runtime 配置
        runtime_filename = f"runtime_{param_name}_{value_str}.json"
        runtime_path = os.path.join(config_subdir, runtime_filename)
        with open(runtime_path, 'w', encoding='utf-8') as f:
            json.dump(new_runtime, f, indent=2, ensure_ascii=False)
        
        # 生成运行命令
        cmd = f"{command_template} {runtime_path}"
        commands.append(cmd)
        
        print(f"  生成: {runtime_filename}")
    
    # 写入 run_simulations.sh
    with open(commands_file, 'w', encoding='utf-8', newline='\n') as f:
        f.write("#!/bin/bash\n")
        f.write(f"# 自动生成的推理仿真脚本\n")
        f.write(f"# Runtime: {runtime_prefix}\n")
        f.write(f"# 参数: {param_path}\n")
        f.write(f"# 共 {len(commands)} 个测试\n\n")
        for cmd in commands:
            f.write(cmd + "\n")
    
    print("-" * 50)
    print(f"配置文件目录: {config_subdir}")
    print(f"运行脚本: {commands_file}")
    print(f"共生成 {len(commands)} 条命令")

if __name__ == "__main__":
    generate()
