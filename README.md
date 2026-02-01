# Automatic Test - 自动化测试与数据分析工具集

用于机器学习模型推理和训练仿真的自动化测试、参数扫描与数据整合工具集。

## 项目结构

```
Automatic_test/
├── inference_automatic/       # 推理自动化测试
├── training_automatic/        # 训练自动化测试
├── data_integration/          # 数据整合分析
├── 推理资料.md                 # 推理仿真文档
└── 训练资料.md                 # 训练仿真文档
```

---

## 模块说明

### 1. inference_automatic - 推理自动化测试

自动生成推理仿真配置并执行参数扫描。

```bash
cd inference_automatic
python generate_tests.py       # 生成测试配置
python analyze_results.py      # 分析结果
```

### 2. training_automatic - 训练自动化测试

自动生成训练仿真配置并执行参数扫描。

```bash
cd training_automatic
python generate_tests.py       # 生成测试配置
python analyze_results.py      # 分析结果
```

### 3. data_integration - 数据整合分析

基于配置文件的灵活数据分析系统，整合推理和训练仿真结果。

**功能特性**：
- ✅ 灵活字段选择 - 通过 YAML 配置自定义分析维度
- ✅ 数据过滤 - 支持多种操作符
- ✅ 透视表/扁平表 - 可选输出模式
- ✅ 多文件批处理

**使用方法**：

```bash
cd data_integration

# 推理数据整合
python inference_integration.py

# 训练数据整合
python training_integration.py
```

**配置文件**：
- `inference_config.yaml` - 推理数据配置
- `training_config.yaml` - 训练数据配置

详细说明请参考 [data_integration/README.md](data_integration/README.md)

---

## 依赖安装

```bash
pip install pandas numpy pyyaml openpyxl
```

---

## License

MIT
