# 数据整合工具 (Data Integration)

基于配置文件的灵活数据分析系统，用于整合推理和训练仿真结果。

## 功能特性

- ✅ **灵活字段选择** - 通过 YAML 配置自定义分析维度
- ✅ **数据过滤** - 支持 `==`, `!=`, `>`, `<`, `>=`, `<=`, `in`, `not_in` 操作符
- ✅ **透视表/扁平表** - 可选输出模式
- ✅ **多文件批处理** - 自动扫描目录下所有匹配文件
- ✅ **智能编码检测** - 自动处理 UTF-8/GBK 等编码

## 目录结构

```
data_integration/
├── inference_config.yaml      # 推理数据配置
├── inference_integration.py   # 推理数据处理脚本
├── training_config.yaml       # 训练数据配置
├── training_integration.py    # 训练数据处理脚本
├── utils.py                   # 共用工具函数
├── raw_data/                  # 原始数据目录
│   ├── inference/             # 推理 CSV 文件
│   └── training/              # 训练 CSV 文件
└── data/                      # 输出目录
    ├── inference/             # 推理分析结果
    └── training/              # 训练分析结果
```

## 快速开始

### 1. 准备数据

将原始 CSV 文件放入对应目录：
- 推理数据 → `raw_data/inference/`
- 训练数据 → `raw_data/training/`

### 2. 修改配置（可选）

编辑配置文件自定义分析参数：
- 推理：`inference_config.yaml`
- 训练：`training_config.yaml`

### 3. 运行脚本

```bash
# 推理数据整合
python inference_integration.py

# 训练数据整合
python training_integration.py
```

### 4. 查看结果

输出文件保存在 `data/` 目录下，格式为 Excel (.xlsx)。

## 配置说明

### 推理数据配置示例

```yaml
# 自变量（分析维度）
independent_variables:
  row_fields:
    - field: "model_name"
      alias: "Model"
    - field: "decoder_system_name"
      alias: "System"
  column_fields:
    - field: "decoder_time_limit(ms)"
      alias: "Time Limit"

# 因变量（吞吐量指标）
dependent_variables:
  - field: "decoder_throughput(token/s)"
    alias: "Decode Total"
    prefix: "Decode"

# 过滤条件
filters:
  - field: "model_name"
    operator: "in"
    values: ["deepseek-v3", "qwen3-32B"]
```

### 训练数据配置示例

```yaml
# 因变量（性能指标）
dependent_variables:
  - field: "throughput_per_proc"
    alias: "Throughput/Proc"
  - field: "total_efficiency"
    alias: "Total Efficiency"

# 分析设置
analysis:
  mode: "flat"                    # flat / pivot / both
  sort_by: "throughput_per_proc"
  top_n_per_file: 1               # 每文件取前 N 条结果
```

## 依赖

```
pandas
numpy
pyyaml
openpyxl
```

安装依赖：
```bash
pip install pandas numpy pyyaml openpyxl
```

## License

MIT
