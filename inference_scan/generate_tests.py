#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推理测试生成脚本
功能：从 config.yaml 读取配置，支持 1~2 个参数的排列组合扫描
      生成多个 runtime/系统 配置文件和 run_simulations.sh 脚本
"""

import os
import json
import copy
from pathlib import Path

from common import (
    load_yaml_config, generate_param_name_from_path, format_value_for_filename,
    parse_scan_params, generate_param_combinations, build_combo_dir_name,
    build_combo_sys_name, build_combo_filename, is_sys_config_param,
    CONFIG_YAML_PATH
)

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
        print(f"    等差数列: start={start}, end={end}, step={step}")
    elif value_type == 'power_of_2':
        start_power = param_mode.get('start_power', 0)
        end_power = param_mode.get('end_power', 10)
        raw_values = generate_power_of_2_values(start_power, end_power)
        print(f"    2的幂次: 2^{start_power} 到 2^{end_power}")
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
    """主生成函数：支持 1~2 个参数的排列组合扫描"""
    # 加载配置
    config = load_yaml_config(CONFIG_YAML_PATH)
    scan_config = config.get('scan', {})
    run_config = config.get('run', {})
    
    # 获取配置路径
    base_runtime_path = scan_config['base_runtime_config']
    base_sys_config_path = scan_config['base_sys_config']
    output_dir = scan_config['output_dir']
    generated_configs_dir = scan_config['generated_configs_dir']
    commands_file = scan_config.get('commands_file', './automatic/run_simulations.sh')
    command_template = run_config.get('command', 'python3 run.py -c')
    
    # 解析扫描参数（兼容新旧配置格式）
    scan_params = parse_scan_params(scan_config)
    n_params = len(scan_params)
    
    if n_params > 2:
        raise NotImplementedError(f"当前仅支持 1~2 个扫描参数，配置了 {n_params} 个")
    
    # 为每个参数生成名称和值列表
    param_paths = []
    param_names = []
    all_values = []
    
    print(f"扫描维度: {n_params}")
    print("=" * 50)
    
    for i, sp in enumerate(scan_params):
        pp = sp['param_path']
        pm = sp.get('param_mode', {})
        pn = generate_param_name_from_path(pp)
        vals = get_test_values(pm)
        
        param_paths.append(pp)
        param_names.append(pn)
        all_values.append(vals)
        
        print(f"  参数 {i+1}: {pp} -> {pn} ({len(vals)} 个值)")
    
    # 笛卡尔积生成所有组合
    combinations = generate_param_combinations(all_values)
    total_tests = len(combinations)
    
    print("=" * 50)
    print(f"总计: {total_tests} 个组合 ({' × '.join(str(len(v)) for v in all_values)})")
    
    # runtime 前缀
    runtime_prefix = Path(base_runtime_path).stem
    
    print(f"基础 Runtime: {base_runtime_path}")
    print(f"基础 SysConfig: {base_sys_config_path}")
    
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
    
    # 判断每个参数是否属于系统配置
    param_in_sys = [is_sys_config_param(pp) for pp in param_paths]
    any_sys_param = any(param_in_sys)
    
    commands = []
    
    for combo in combinations:
        # combo 是一个 tuple，长度 = n_params
        values_list = list(combo)
        
        new_runtime = copy.deepcopy(base_runtime)
        
        # 创建组合目录名
        sub_dir_name = build_combo_dir_name(param_names, values_list)
        specific_output_path = os.path.join(results_base_dir, sub_dir_name)
        os.makedirs(specific_output_path, exist_ok=True)
        
        # 更新 runtime 中的 output 字段
        for deploy_mode in new_runtime.keys():
            if isinstance(new_runtime[deploy_mode], dict) and \
               deploy_mode in ['pd-split-request-optimal', 'pd-fusion']:
                new_runtime[deploy_mode]['output'] = specific_output_path
        
        # 处理系统配置参数
        new_sys_config = copy.deepcopy(base_sys_config) if any_sys_param else None
        
        for i in range(n_params):
            pp = param_paths[i]
            val = values_list[i]
            
            if param_in_sys[i]:
                # 参数在系统配置中
                try:
                    set_param_value(new_sys_config, pp, val)
                except (KeyError, TypeError, IndexError) as e:
                    print(f"  警告: 无法在系统配置中设置参数 {pp}: {e}")
            else:
                # 参数在 runtime 配置中
                try:
                    set_param_value(new_runtime, pp, val)
                except (KeyError, TypeError, IndexError) as e:
                    print(f"  错误: 无法在 runtime 配置中设置参数 {pp}: {e}")
                    raise
        
        # 保存系统配置（如果有系统参数被修改）
        if any_sys_param and new_sys_config is not None:
            new_sys_config['name'] = build_combo_sys_name(original_sys_name, param_names, values_list)
            
            sys_filename = build_combo_filename("sys", param_names, values_list, ".json")
            new_sys_path = os.path.join(config_subdir, sys_filename)
            with open(new_sys_path, 'w', encoding='utf-8') as f:
                json.dump(new_sys_config, f, indent=2, ensure_ascii=False)
            
            # 更新 runtime 中的 sys_list 引用
            for deploy_mode in new_runtime.keys():
                if deploy_mode in ['pd-split-request-optimal', 'pd-fusion']:
                    sys_list = new_runtime[deploy_mode].get('sys_list', [])
                    if isinstance(sys_list, list):
                        for j, sys_item in enumerate(sys_list):
                            if isinstance(sys_item, list):
                                for k, sys_path in enumerate(sys_item):
                                    sys_list[j][k] = new_sys_path
                            elif sys_item == base_sys_config_path:
                                sys_list[j] = new_sys_path
        
        # 保存 runtime 配置
        runtime_filename = build_combo_filename("runtime", param_names, values_list, ".json")
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
        for i, pp in enumerate(param_paths):
            f.write(f"# 参数{i+1}: {pp} ({len(all_values[i])} 个值)\n")
        f.write(f"# 共 {len(commands)} 个测试")
        if n_params > 1:
            f.write(f" ({' × '.join(str(len(v)) for v in all_values)})")
        f.write("\n\n")
        for cmd in commands:
            f.write(cmd + "\n")
    
    print("-" * 50)
    print(f"配置文件目录: {config_subdir}")
    print(f"运行脚本: {commands_file}")
    print(f"共生成 {len(commands)} 条命令")

if __name__ == "__main__":
    generate()
