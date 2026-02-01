#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推理测试生成脚本
功能：从 config.yaml 读取配置，生成系统配置文件和 run_simulations.sh 脚本
"""

import json
import os
import copy
import yaml

# 配置文件路径
CONFIG_YAML_PATH = "./automatic/config.yaml"


def load_config():
    """加载配置文件"""
    with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_test_values(conf):
    """
    根据配置生成测试值列表
    
    支持两种模式：
    - range: 等差数列 (start, stop, step)
    - power_of_2: 2的幂次 (start_power, end_power)
    """
    ts = conf['test_setting']
    mode = ts.get('mode', 'range')
    
    if mode == "range":
        start = ts.get('start', 1)
        stop = ts.get('stop', 10)
        step = ts.get('step', 1)
        return list(range(start, stop + 1, step))
    elif mode == "power_of_2":
        start_power = ts.get('start_power', 0)
        end_power = ts.get('end_power', 10)
        return [2**i for i in range(start_power, end_power + 1)]
    elif mode == "linspace":
        start = ts.get('start', 1)
        stop = ts.get('stop', 10)
        num = ts.get('num', 10)
        return [round(start + (stop - start) * i / (num - 1), 4) for i in range(num)]
    else:
        raise ValueError(f"不支持的模式: {mode}")


def set_nested_value(data, path, value):
    """
    设置嵌套字典中的值
    
    Args:
        data: 目标字典
        path: 路径列表，如 ["mem1", "GiB"]
        value: 要设置的值
    """
    curr = data
    for key in path[:-1]:
        curr = curr[key]
    curr[path[-1]] = value


def generate():
    """主生成函数"""
    conf = load_config()
    
    paths = conf['paths']
    ts = conf['test_setting']
    
    # 获取运行时配置前缀名
    runtime_prefix = os.path.splitext(os.path.basename(paths['base_runtime_config']))[0]
    param_name = ts['param_name']
    param_path = ts['param_path']
    
    # 加载基础系统配置
    with open(paths['base_sys_config'], 'r', encoding='utf-8') as f:
        base_system = json.load(f)
    
    # 创建测试文件夹名称
    test_folder_name = f"{runtime_prefix}_{param_name}_scan"
    current_output_dir = os.path.join(paths['output_root'], test_folder_name)
    
    # 确保目录存在
    os.makedirs(paths['system_gen_dir'], exist_ok=True)
    os.makedirs(current_output_dir, exist_ok=True)
    
    # 生成测试值
    test_values = get_test_values(conf)
    print(f"生成 {len(test_values)} 个测试值: {test_values[:5]}{'...' if len(test_values) > 5 else ''}")
    
    commands = []
    
    for val in test_values:
        # 深拷贝基础配置
        new_system = copy.deepcopy(base_system)
        
        # 设置新参数值
        set_nested_value(new_system, param_path, val)
        
        # 更新配置名称
        new_system["name"] = f"{runtime_prefix}_{param_name}_{val}"
        
        # 生成系统配置文件
        sys_filename = f"sys_{runtime_prefix}_{param_name}_{val}.json"
        sys_path = os.path.join(paths['system_gen_dir'], sys_filename)
        with open(sys_path, 'w', encoding='utf-8') as f:
            json.dump(new_system, f, indent=2)
        
        # 生成输出目录（每个测试一个子目录）
        result_subdir = f"{runtime_prefix}_{param_name}_{val}"
        output_dir = os.path.join(current_output_dir, result_subdir)
        
        # 构建运行命令
        cmd = (
            f"python3 run.py "
            f"-c {paths['base_runtime_config']} "
            f"-s {sys_path} "
            f"-o {output_dir}"
        )
        commands.append(cmd)
    
    # 写入 run_simulations.sh
    commands_file = paths.get('commands_file', './automatic/run_simulations.sh')
    with open(commands_file, 'w', encoding='utf-8', newline='\n') as f:
        f.write("#!/bin/bash\n")
        f.write(f"# 自动生成的推理仿真脚本\n")
        f.write(f"# 参数: {param_name}, 范围: {test_values[0]} - {test_values[-1]}\n")
        f.write(f"# 共 {len(commands)} 个测试\n\n")
        f.write("\n".join(commands))
        f.write("\n")
    
    print("-" * 50)
    print(f"生成完成！")
    print(f"  系统配置目录: {paths['system_gen_dir']}")
    print(f"  输出目录: {current_output_dir}")
    print(f"  运行脚本: {commands_file}")
    print(f"  测试数量: {len(commands)}")
    print("-" * 50)
    print(f"\n运行以下命令开始测试:")
    print(f"  bash {commands_file}")


if __name__ == "__main__":
    generate()
