"""
训练数据整合模块
Training Data Integration Module

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
    format_output_filename
)


class TrainingDataIntegration:
    """训练数据整合处理器"""
    
    def __init__(self, config_path: str = "training_config.yaml"):
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
        
        训练数据的 CSV 是标准格式（首行为表头，每行为一条记录）
        
        Returns:
            合并后的 DataFrame
        """
        input_config = self.config.get('input', {})
        
        # 检查是否指定了单个文件
        single_file = input_config.get('single_file')
        if single_file and os.path.exists(single_file):
            print(f"正在加载单个文件: {single_file}")
            df = self._load_single_file(single_file)
            df['_source_file'] = os.path.basename(single_file)
            self.data = df
            return self.data
        
        # 扫描目录下的文件
        source_dir = input_config.get('source_dir', './raw_data/training/')
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
                print(f"  ✓ 已加载: {os.path.basename(file_path)} ({len(df)} 行)")
            except Exception as e:
                print(f"  ✗ 加载失败: {os.path.basename(file_path)} - {e}")
        
        if not all_data:
            raise ValueError("没有成功加载任何数据")
        
        self.data = pd.concat(all_data, ignore_index=True)
        print(f"总计加载 {len(self.data)} 条记录")
        
        return self.data
    
    def _load_single_file(self, file_path: str) -> pd.DataFrame:
        """
        加载单个 CSV 文件
        
        训练数据是标准 CSV 格式
        
        Args:
            file_path: 文件路径
            
        Returns:
            DataFrame
        """
        df = smart_read_csv(file_path)
        
        # 获取每个文件取前 N 行的配置
        analysis_config = self.config.get('analysis', {})
        top_n = analysis_config.get('top_n_per_file')
        
        if top_n and top_n > 0 and len(df) > top_n:
            # 按主要指标排序后取前 N 行
            sort_by = analysis_config.get('sort_by', 'throughput_per_proc')
            sort_order = analysis_config.get('sort_order', 'descending')
            
            if sort_by in df.columns:
                df[sort_by] = pd.to_numeric(df[sort_by], errors='coerce')
                ascending = sort_order == 'ascending'
                df = df.sort_values(sort_by, ascending=ascending).head(top_n)
        
        return df
    
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
        
        # 附加字段
        fields.extend(self.config.get('additional_fields', []))
        
        return [f for f in fields if f]
    
    def build_flat_table(self) -> pd.DataFrame:
        """
        构建扁平表格（训练数据的主要输出模式）
        
        Returns:
            扁平表格 DataFrame
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        
        df = self.data.copy()
        
        # 选择要输出的字段
        output_fields = []
        
        # 添加行字段（通常是源文件）
        iv_config = self.config.get('independent_variables', {})
        for f in iv_config.get('row_fields', []):
            field = f.get('field')
            if field and field in df.columns:
                output_fields.append(field)
        
        # 添加分组字段
        for field in iv_config.get('group_fields', []):
            if field in df.columns:
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
        
        # 如果没有找到任何配置的字段，使用所有列
        if not output_fields:
            output_fields = list(df.columns)
        
        result = df[output_fields].copy()
        
        # 排序
        analysis_config = self.config.get('analysis', {})
        sort_by = analysis_config.get('sort_by')
        sort_order = analysis_config.get('sort_order', 'descending')
        
        if sort_by and sort_by in result.columns:
            ascending = sort_order == 'ascending'
            result = result.sort_values(sort_by, ascending=ascending)
        
        # 四舍五入
        decimal_places = analysis_config.get('decimal_places', 4)
        numeric_cols = result.select_dtypes(include=[np.number]).columns
        result[numeric_cols] = result[numeric_cols].round(decimal_places)
        
        # 重命名列（使用别名）
        rename_map = {}
        for f in iv_config.get('row_fields', []):
            field = f.get('field')
            alias = f.get('alias')
            if field and alias and field in result.columns:
                rename_map[field] = alias
        
        for dv in self.config.get('dependent_variables', []):
            field = dv.get('field')
            alias = dv.get('alias')
            if field and alias and field in result.columns:
                rename_map[field] = alias
        
        result = result.rename(columns=rename_map)
        
        return result
    
    def build_pivot_table(self) -> pd.DataFrame:
        """
        构建透视表（可选输出模式）
        
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
        
        if not row_fields:
            # 如果没有指定行字段，使用源文件作为索引
            if '_source_file' in df.columns:
                row_fields = ['_source_file']
            else:
                row_fields = [df.columns[0]]
        
        # 获取因变量
        dependent_vars = self.config.get('dependent_variables', [])
        
        # 构建透视表
        value_fields = [dv.get('field') for dv in dependent_vars if dv.get('field') in df.columns]
        
        if not value_fields:
            raise ValueError("没有找到有效的因变量字段")
        
        # 按行字段分组，取第一行的值
        result = df.groupby(row_fields)[value_fields].first().reset_index()
        
        # 重命名列
        rename_map = {}
        for f in iv_config.get('row_fields', []):
            field = f.get('field')
            alias = f.get('alias')
            if field and alias and field in result.columns:
                rename_map[field] = alias
        
        for dv in dependent_vars:
            field = dv.get('field')
            alias = dv.get('alias')
            if field and alias and field in result.columns:
                rename_map[field] = alias
        
        result = result.rename(columns=rename_map)
        
        # 四舍五入
        decimal_places = analysis_config.get('decimal_places', 4)
        numeric_cols = result.select_dtypes(include=[np.number]).columns
        result[numeric_cols] = result[numeric_cols].round(decimal_places)
        
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
        output_dir = output_config.get('dir', './data/training/')
        filename_template = output_config.get('filename', 'training_analysis_{timestamp}.xlsx')
        
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
        print("训练数据整合处理开始")
        print("=" * 50)
        
        # 1. 加载数据
        print("\n[1/4] 加载数据...")
        self.load_data()
        
        # 2. 预处理
        print("\n[2/4] 数据预处理...")
        self.preprocess()
        
        # 3. 构建输出表格
        print("\n[3/4] 构建分析表格...")
        analysis_mode = self.config.get('analysis', {}).get('mode', 'flat')
        
        output_files = []
        
        if analysis_mode in ('flat', 'both'):
            print("  构建扁平表...")
            flat_df = self.build_flat_table()
            output_files.append(self.export(flat_df, 'flat' if analysis_mode == 'both' else ''))
        
        if analysis_mode in ('pivot', 'both'):
            print("  构建透视表...")
            pivot_df = self.build_pivot_table()
            output_files.append(self.export(pivot_df, 'pivot' if analysis_mode == 'both' else ''))
        
        # 4. 完成
        print("\n[4/4] 处理完成!")
        print("=" * 50)
        
        return tuple(output_files)


def main():
    """主函数"""
    # 确定配置文件路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'training_config.yaml')
    
    # 如果命令行指定了配置文件
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    try:
        processor = TrainingDataIntegration(config_path)
        processor.run()
    except Exception as e:
        print(f"\n错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
