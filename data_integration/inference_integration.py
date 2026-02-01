"""
推理数据整合模块
Inference Data Integration Module

基于配置文件的灵活数据分析系统
Configuration-driven flexible data analysis system
"""

import os
import sys
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple

from utils import (
    load_config,
    smart_read_csv,
    ensure_dir,
    find_files,
    apply_filters,
    transpose_wide_format,
    format_output_filename
)


class InferenceDataIntegration:
    """推理数据整合处理器"""
    
    def __init__(self, config_path: str = "inference_config.yaml"):
        """
        初始化处理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = load_config(config_path)
        self.data: Optional[pd.DataFrame] = None
        
    def load_data(self) -> pd.DataFrame:
        """
        加载并合并所有输入数据
        
        Returns:
            合并后的 DataFrame
        """
        input_config = self.config.get('input', {})
        
        # 检查是否指定了单个文件
        single_file = input_config.get('single_file')
        if single_file and os.path.exists(single_file):
            print(f"正在加载单个文件: {single_file}")
            return self._load_single_file(single_file)
        
        # 扫描目录下的文件
        source_dir = input_config.get('source_dir', './raw_data/inference/')
        file_pattern = input_config.get('file_pattern', '*.csv')
        
        files = find_files(source_dir, file_pattern)
        
        if not files:
            raise FileNotFoundError(f"未找到匹配的文件: {source_dir}/{file_pattern}")
        
        print(f"找到 {len(files)} 个文件")
        
        all_data = []
        for file_path in files:
            try:
                df = self._load_single_file(file_path)
                # 添加源文件信息
                df['_source_file'] = os.path.basename(file_path)
                all_data.append(df)
                print(f"  ✓ 已加载: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"  ✗ 加载失败: {os.path.basename(file_path)} - {e}")
        
        if not all_data:
            raise ValueError("没有成功加载任何数据")
        
        self.data = pd.concat(all_data, ignore_index=True)
        print(f"总计加载 {len(self.data)} 条记录")
        
        return self.data
    
    def _load_single_file(self, file_path: str) -> pd.DataFrame:
        """
        加载单个 CSV 文件（处理宽格式转置）
        
        Args:
            file_path: 文件路径
            
        Returns:
            转置后的 DataFrame
        """
        raw_df = smart_read_csv(file_path, header=None)
        
        # 检查是否为宽格式（首列包含 field_name）
        first_col = raw_df.iloc[:, 0].astype(str)
        if 'field_name' in first_col.values or 'model_name' in first_col.values:
            # 宽格式，需要转置
            return transpose_wide_format(raw_df)
        else:
            # 标准格式，直接返回
            return smart_read_csv(file_path)
    
    def preprocess(self) -> pd.DataFrame:
        """
        数据预处理：过滤、类型转换
        
        Returns:
            预处理后的 DataFrame
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        
        df = self.data.copy()
        
        # 应用过滤器
        filters = self.config.get('filters', [])
        if filters:
            original_count = len(df)
            df = apply_filters(df, filters)
            print(f"过滤器应用: {original_count} -> {len(df)} 条记录")
        
        # 数值类型转换
        numeric_fields = self._get_numeric_fields()
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        self.data = df
        return df
    
    def _get_numeric_fields(self) -> List[str]:
        """获取所有需要转为数值的字段"""
        fields = []
        
        # 从因变量获取
        for dv in self.config.get('dependent_variables', []):
            fields.append(dv.get('field'))
        
        # 从自变量中的列字段获取
        iv_config = self.config.get('independent_variables', {})
        for col_field in iv_config.get('column_fields', []):
            fields.append(col_field.get('field'))
        
        # 附加字段
        fields.extend(self.config.get('additional_fields', []))
        
        return [f for f in fields if f]
    
    def build_pivot_table(self) -> pd.DataFrame:
        """
        构建透视表
        
        Returns:
            透视表 DataFrame
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        
        df = self.data.copy()
        iv_config = self.config.get('independent_variables', {})
        analysis_config = self.config.get('analysis', {})
        
        # 获取行索引字段
        row_fields = [f.get('field') for f in iv_config.get('row_fields', [])]
        row_fields = [f for f in row_fields if f and f in df.columns]
        
        # 获取列字段
        col_fields = [f.get('field') for f in iv_config.get('column_fields', [])]
        col_fields = [f for f in col_fields if f and f in df.columns]
        
        if not row_fields:
            print("警告: 未找到有效的行索引字段")
            row_fields = [df.columns[0]]
        
        # 检查是否启用派生行
        derived_config = analysis_config.get('derived_rows', {})
        if derived_config.get('enabled', False):
            df = self._create_derived_rows(df, derived_config)
            row_fields.append('_metric_label')
        
        # 获取因变量
        dependent_vars = self.config.get('dependent_variables', [])
        
        # 为每个因变量创建透视表块
        pivot_blocks = []
        for dv in dependent_vars:
            field = dv.get('field')
            prefix = dv.get('prefix', dv.get('alias', field))
            
            if field not in df.columns:
                print(f"警告: 因变量字段 '{field}' 不存在，已跳过")
                continue
            
            # 创建透视表
            if col_fields:
                pivot = df.pivot_table(
                    index=row_fields,
                    columns=col_fields[0],  # 目前只支持单列透视
                    values=field,
                    aggfunc='first'
                )
                
                # 列排序（降序）
                sorted_cols = sorted(pivot.columns, reverse=True)
                pivot = pivot.reindex(columns=sorted_cols)
                
                # 列名美化
                decimal_places = analysis_config.get('decimal_places', 0)
                pivot = pivot.round(decimal_places)
                pivot.columns = [f'{prefix}_{int(c)}ms' if isinstance(c, (int, float)) else f'{prefix}_{c}' 
                                for c in pivot.columns]
            else:
                # 无列透视，直接作为值列
                pivot = df.set_index(row_fields)[[field]]
                pivot.columns = [prefix]
            
            pivot_blocks.append(pivot)
        
        if not pivot_blocks:
            raise ValueError("没有成功创建任何透视表块")
        
        # 水平合并所有块
        result = pd.concat(pivot_blocks, axis=1)
        
        # 整理索引
        result = result.reset_index()
        
        # 重命名索引列
        rename_map = {}
        for field_config in iv_config.get('row_fields', []):
            field = field_config.get('field')
            alias = field_config.get('alias')
            if field and alias and field in result.columns:
                rename_map[field] = alias
        
        if '_metric_label' in result.columns:
            rename_map['_metric_label'] = 'Metric'
        
        result = result.rename(columns=rename_map)
        
        return result
    
    def _create_derived_rows(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """
        创建派生行（单卡 vs 整机）
        
        Args:
            df: 原始 DataFrame
            config: 派生行配置
            
        Returns:
            包含派生行的 DataFrame
        """
        npu_field = config.get('npu_count_field', 'decoder_num_npu')
        total_field = config.get('total_throughput_field', 'decoder_throughput(token/s)')
        per_npu_field = config.get('per_npu_throughput_field', 'decoder_throughput_per_npu(token/s)')
        
        # 创建整机行
        df_total = df.copy()
        if npu_field in df_total.columns:
            df_total['_metric_label'] = df_total[npu_field].apply(lambda x: f'{int(x)}卡' if pd.notna(x) else '整机')
        else:
            df_total['_metric_label'] = '整机'
        df_total['_sort_order'] = 0
        
        # 创建单卡行
        df_single = df.copy()
        df_single['_metric_label'] = '单卡'
        df_single['_sort_order'] = 1
        
        # 合并
        combined = pd.concat([df_total, df_single], ignore_index=True)
        
        # 按排序顺序排列
        combined = combined.sort_values('_sort_order').drop(columns=['_sort_order'])
        
        return combined
    
    def build_flat_table(self) -> pd.DataFrame:
        """
        构建扁平表格（不透视）
        
        Returns:
            扁平表格 DataFrame
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        
        df = self.data.copy()
        
        # 选择要输出的字段
        output_fields = []
        
        # 添加行字段
        iv_config = self.config.get('independent_variables', {})
        for f in iv_config.get('row_fields', []):
            field = f.get('field')
            if field and field in df.columns:
                output_fields.append(field)
        
        # 添加列字段
        for f in iv_config.get('column_fields', []):
            field = f.get('field')
            if field and field in df.columns:
                output_fields.append(field)
        
        # 添加因变量
        for dv in self.config.get('dependent_variables', []):
            field = dv.get('field')
            if field and field in df.columns:
                output_fields.append(field)
        
        # 添加附加字段
        for field in self.config.get('additional_fields', []):
            if field in df.columns:
                output_fields.append(field)
        
        # 去重保持顺序
        output_fields = list(dict.fromkeys(output_fields))
        
        result = df[output_fields].copy()
        
        # 排序
        analysis_config = self.config.get('analysis', {})
        sort_by = analysis_config.get('sort_by')
        sort_order = analysis_config.get('sort_order', 'descending')
        
        if sort_by and sort_by in result.columns:
            ascending = sort_order == 'ascending'
            result = result.sort_values(sort_by, ascending=ascending)
        
        return result
    
    def export(self, df: pd.DataFrame, suffix: str = "") -> str:
        """
        导出结果到 Excel
        
        Args:
            df: 要导出的 DataFrame
            suffix: 文件名后缀
            
        Returns:
            输出文件路径
        """
        output_config = self.config.get('output', {})
        output_dir = output_config.get('dir', './data/inference/')
        filename_template = output_config.get('filename', 'inference_analysis_{timestamp}.xlsx')
        
        ensure_dir(output_dir)
        
        filename = format_output_filename(filename_template)
        if suffix:
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{suffix}{ext}"
        
        output_path = os.path.join(output_dir, filename)
        
        df.to_excel(output_path, index=False)
        print(f"结果已导出: {output_path}")
        
        return output_path
    
    def run(self) -> Tuple[str, ...]:
        """
        执行完整处理流程
        
        Returns:
            输出文件路径元组
        """
        print("=" * 50)
        print("推理数据整合处理开始")
        print("=" * 50)
        
        # 1. 加载数据
        print("\n[1/4] 加载数据...")
        self.load_data()
        
        # 2. 预处理
        print("\n[2/4] 数据预处理...")
        self.preprocess()
        
        # 3. 构建输出表格
        print("\n[3/4] 构建分析表格...")
        analysis_mode = self.config.get('analysis', {}).get('mode', 'pivot')
        
        output_files = []
        
        if analysis_mode in ('pivot', 'both'):
            print("  构建透视表...")
            pivot_df = self.build_pivot_table()
            output_files.append(self.export(pivot_df, 'pivot' if analysis_mode == 'both' else ''))
        
        if analysis_mode in ('flat', 'both'):
            print("  构建扁平表...")
            flat_df = self.build_flat_table()
            output_files.append(self.export(flat_df, 'flat' if analysis_mode == 'both' else ''))
        
        # 4. 完成
        print("\n[4/4] 处理完成!")
        print("=" * 50)
        
        return tuple(output_files)


def main():
    """主函数"""
    # 确定配置文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'inference_config.yaml')
    
    # 如果命令行指定了配置文件
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    try:
        processor = InferenceDataIntegration(config_path)
        processor.run()
    except Exception as e:
        print(f"\n错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
