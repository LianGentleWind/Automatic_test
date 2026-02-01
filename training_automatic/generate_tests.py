import json
import os
import yaml
import copy

def get_test_values(conf):
    ts = conf['test_setting']
    mode = ts['mode']
    if mode == "range":
        return list(range(ts['start'], ts['stop'] + 1, ts.get('step', 1)))
    elif mode == "linspace":
        start, stop, num = ts['start'], ts['stop'], ts['num']
        return [round(start + (stop - start) * i / (num - 1), 4) for i in range(num)]
    return []

def set_nested_value(data, path, value):
    curr = data
    for key in path[:-1]:
        curr = curr[key]
    curr[path[-1]] = value

def generate():
    with open("./automatic/config.yaml", "r") as f:
        conf = yaml.safe_load(f)
    
    paths = conf['paths']
    ts = conf['test_setting']

    runtime_prefix = os.path.splitext(os.path.basename(paths['base_runtime']))[0]
    param_name = ts['param_name']
    
    with open(paths['base_system'], 'r') as f:
        base_system = json.load(f)
    
    test_folder_name = f"{runtime_prefix}_{param_name}_scan"
    current_output_dir = os.path.join(paths['output_root'], test_folder_name)

    os.makedirs(paths['system_gen_dir'], exist_ok=True)
    os.makedirs(current_output_dir, exist_ok=True)

    test_values = get_test_values(conf)
    commands = []

    for val in test_values:
        new_system = copy.deepcopy(base_system)
        set_nested_value(new_system, ts['param_path'], val)
        new_system["name"] = f"{runtime_prefix}_{param_name}_{val}"
        
        sys_filename = f"sys_{runtime_prefix}_{param_name}_{val}.json"
        sys_path = os.path.join(paths['system_gen_dir'], sys_filename)
        with open(sys_path, 'w') as f:
            json.dump(new_system, f, indent=2)


        res_filename = f"{runtime_prefix}_{param_name}_{val}.csv"
        output_csv = os.path.join(current_output_dir, res_filename)
        
        cmd = (
            f"./bin/calculon lco "
            f"--model {paths['base_model']} "
            f"--runtime {paths['base_runtime']} "
            f"--system {sys_path} "
            f"--output {output_csv} "
            f"-c 90"
        )
        commands.append(cmd)

    with open(paths['commands_file'], 'w') as f:
        f.write("#!/bin/bash\n" + "\n".join(commands))
    
    print(f"命令已生成。输出文件示例: {runtime_prefix}_{param_name}_{val}.csv")

if __name__ == "__main__":
    generate()
