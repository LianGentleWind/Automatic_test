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
            # 标准格式，设置首行为列名并返回（复用已读取的数据）
            raw_df.columns = raw_df.iloc[0]
            return raw_df.iloc[1:].reset_index(drop=True)
    
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
        构建透视表：支持多维列、修复重复列、数值排序及归一化
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        
        df_raw = self.data.copy()
        iv_config = self.config.get('independent_variables', {})
        analysis_config = self.config.get('analysis', {})
        dependent_vars = self.config.get('dependent_variables', [])
        
        # 1. 指标逻辑分组：将相同 prefix 的 Total 和 Per NPU 归为一类
        metric_groups = {}
        for dv in dependent_vars:
            prefix = dv.get('prefix', dv.get('alias', dv.get('field')))
            if prefix not in metric_groups:
                metric_groups[prefix] = {"total": None, "single": None}
            field = dv.get('field')
            alias = str(dv.get('alias', '')).lower()
            if 'per_npu' in field.lower() or 'single' in alias or 'per npu' in alias:
                metric_groups[prefix]["single"] = field
            else:
                metric_groups[prefix]["total"] = field

        # 2. 准备行索引和数据对齐
        row_fields = [f.get('field') for f in iv_config.get('row_fields', [])]
        row_fields = [f for f in row_fields if f and f in df_raw.columns]
        
        derived_config = analysis_config.get('derived_rows', {})
        if derived_config.get('enabled', False):
            npu_field = derived_config.get('npu_count_field', 'decoder_num_npu')
            
            # 整机/多卡行：标记为 'total' 类型
            df_total = df_raw.copy()
            npu_vals = pd.to_numeric(df_total[npu_field], errors='coerce')
            df_total['_metric_label'] = npu_vals.apply(lambda x: f'{int(x)}卡' if pd.notna(x) else '整机')
            df_total['_row_type'] = 'total' # 用于归一化对齐
            for prefix, m in metric_groups.items():
                if m["total"]: df_total[f"_val_{prefix}"] = df_total[m["total"]]
            df_total['_sort_order'] = 0
            
            # 单卡行：标记为 'single' 类型
            df_single = df_raw.copy()
            df_single['_metric_label'] = '单卡'
            df_single['_row_type'] = 'single'
            for prefix, m in metric_groups.items():
                source_field = m["single"] if m["single"] else m["total"]
                if source_field: df_single[f"_val_{prefix}"] = df_single[source_field]
            df_single['_sort_order'] = 1
            
            df = pd.concat([df_total, df_single], ignore_index=True)
            row_fields_ext = row_fields + ['_metric_label', '_sort_order', '_row_type']
            pivot_targets = [(prefix, f"_val_{prefix}") for prefix in metric_groups.keys()]
        else:
            df = df_raw.copy()
            df['_row_type'] = 'default'
            row_fields_ext = row_fields + ['_row_type']
            pivot_targets = [(dv.get('prefix', dv.get('alias', dv.get('field'))), dv.get('field')) for dv in dependent_vars]

        # 3. 构建多维列透视
        col_fields = [f.get('field') for f in iv_config.get('column_fields', [])]
        col_fields = [f for f in col_fields if f and f in df.columns]
        
        pivot_blocks = []
        for prefix, target_field in pivot_targets:
            if target_field not in df.columns: continue
            
            # Prefill 是固定值，不随 TPOT (time_limit) 变化，只输出单列
            is_prefill = 'prefill' in prefix.lower()
            
            if col_fields and not is_prefill:
                # Decode 指标：按 time_limit 展开多列
                pivot = df.pivot_table(index=row_fields_ext, columns=col_fields, values=target_field, aggfunc='first')
                pivot = pivot.reindex(columns=sorted(pivot.columns, reverse=True))
                new_cols = []
                for col_tuple in pivot.columns:
                    vals = col_tuple if isinstance(col_tuple, (list, tuple)) else [col_tuple]
                    col_label = "_".join([str(v) for v in vals])
                    unit = "ms" if any(kw in str(col_fields).lower() for kw in ['time', 'limit']) else ""
                    new_cols.append(f"{prefix}_{col_label}{unit}")
                pivot.columns = new_cols
            else:
                # Prefill 指标或无列字段：只输出单列（取第一个值）
                pivot = df.groupby(row_fields_ext)[target_field].first().to_frame()
                pivot.columns = [prefix]
            pivot_blocks.append(pivot)

        # 4. 合并结果并强制进行数值化（解决 Input Length 10 < 9 的排序问题）
        result = pd.concat(pivot_blocks, axis=1).reset_index()
        
        for field in row_fields:
            if field in result.columns:
                # 尝试转换为数字，确保排序正确
                s = pd.to_numeric(result[field], errors='coerce')
                result[field] = s.fillna(result[field])

        # 5. 归一化计算：基于 _row_type 匹配，解决 128卡 vs 8卡 匹配失败的问题
        baseline_system = analysis_config.get('normalization_baseline')
        # 查找 system 字段（归一化和排序共用）
        system_field = next((f.get('field') for f in iv_config.get('row_fields', []) if 'system' in f.get('field', '').lower()), None)
        
        if baseline_system and system_field and system_field in result.columns:
            data_cols = [c for c in result.columns if c not in row_fields_ext and c != system_field]
            # 关键：使用 _row_type (total/single) 进行匹配，而不是具体的 _metric_label
            match_cols = [c for c in row_fields_ext if c not in [system_field, '_sort_order', '_metric_label']]
            
            baseline_df = result[result[system_field] == baseline_system].copy()
            
            if not baseline_df.empty:
                for col in data_cols:
                    norm_col_name = f"{col}"
                    temp_baseline = baseline_df[match_cols + [col]].rename(columns={col: '_base_val'})
                    # 通过 match_cols (Model, Input Length, _row_type) 关联
                    merged = pd.merge(result[match_cols], temp_baseline, on=match_cols, how='left')
                    result[norm_col_name] = (result[col] / merged['_base_val']).round(2)

        # 6. 自定义列排序：Prefill 在 Decode 之前
        metric_order = analysis_config.get('metric_order', [])
        if metric_order:
            data_cols = [c for c in result.columns if c not in row_fields_ext]
            ordered_cols = []
            for prefix in metric_order:
                for col in data_cols:
                    if col.startswith(prefix) and col not in ordered_cols:
                        ordered_cols.append(col)
            # 添加未匹配的列
            for col in data_cols:
                if col not in ordered_cols:
                    ordered_cols.append(col)
            # 重新排列列顺序
            result = result[list(row_fields_ext) + ordered_cols]
        
        # 7. 自定义行排序：按 system_order 排序（支持 *POR*, *LEG* 模式）
        system_order = analysis_config.get('system_order', [])
        # system_field 已在归一化阶段查找，此处复用
        
        if system_order and system_field and system_field in result.columns:
            def get_system_sort_key(system_name):
                """根据 system_order 配置返回排序优先级"""
                for idx, pattern in enumerate(system_order):
                    if pattern.startswith('*') and pattern.endswith('*'):
                        # contains 模式: *POR* -> 包含 "POR"
                        keyword = pattern[1:-1].upper()
                        if keyword in str(system_name).upper():
                            return idx
                    elif pattern.startswith('*'):
                        # 后缀模式: *_POR
                        suffix = pattern[1:].upper()
                        if str(system_name).upper().endswith(suffix):
                            return idx
                    elif pattern.endswith('*'):
                        # 前缀模式: POR_*
                        prefix = pattern[:-1].upper()
                        if str(system_name).upper().startswith(prefix):
                            return idx
                    else:
                        # 精确匹配
                        if str(system_name).upper() == pattern.upper():
                            return idx
                return len(system_order)  # 未匹配的排最后
            
            result['_system_sort'] = result[system_field].apply(get_system_sort_key)
            sort_keys = ['_system_sort'] + row_fields + (['_sort_order'] if '_sort_order' in result.columns else [])
            result = result.sort_values(by=sort_keys, ascending=True)
            result = result.drop(columns=['_system_sort'])
        else:
            # 默认排序
            sort_keys = row_fields + (['_sort_order'] if '_sort_order' in result.columns else [])
            result = result.sort_values(by=sort_keys, ascending=True)
        
        # 8. 清理内部辅助列
        cols_to_drop = [c for c in ['_sort_order', '_row_type'] if c in result.columns]
        result = result.drop(columns=cols_to_drop)
            
        rename_map = {f.get('field'): f.get('alias') for f in iv_config.get('row_fields', [])}
        rename_map['_metric_label'] = 'Metric'
        
        return result.rename(columns=rename_map)

    
    def build_flat_table(self) -> pd.DataFrame:
        """
        构建扁平表：包含额外字段并支持数值化排序
        """
        if self.data is None:
            raise ValueError("请先调用 load_data() 加载数据")
            
        df = self.data.copy()
        iv_config = self.config.get('independent_variables', {})
        dependent_vars = self.config.get('dependent_variables', [])
        # 获取配置中的额外字段列表
        additional_fields = self.config.get('additional_fields', [])
        
        # 1. 提取所有涉及的原始字段名
        row_fields = [f.get('field') for f in iv_config.get('row_fields', [])]
        col_fields = [f.get('field') for f in iv_config.get('column_fields', [])]
        dep_fields = [f.get('field') for f in dependent_vars]
        
        # 按照：行字段 -> 列字段 -> 额外字段 -> 指标字段 的顺序组合
        display_order = row_fields + col_fields + additional_fields + dep_fields
        
        # 过滤掉数据集中不存在的字段，并去重
        all_display_cols = []
        for f in display_order:
            if f and f in df.columns and f not in all_display_cols:
                all_display_cols.append(f)
        
        result = df[all_display_cols].copy()

        # 2. 数值化转换，确保排序逻辑正确（解决 10 > 9 的问题）
        # 对行索引涉及的字段进行转换，确保按照数值大小排序
        for field in row_fields:
            if field in result.columns:
                s = pd.to_numeric(result[field], errors='coerce')
                result[field] = s.fillna(result[field])

        # 3. 执行升序排列
        # 按照 row_fields 里的定义顺序进行多级排序
        valid_sort_keys = [f for f in row_fields if f in result.columns]
        if valid_sort_keys:
            result = result.sort_values(by=valid_sort_keys, ascending=True)

        # 4. 重命名列名为 Alias（如果定义了的话）
        rename_map = {}
        for f in iv_config.get('row_fields', []):
            rename_map[f.get('field')] = f.get('alias')
        for f in iv_config.get('column_fields', []):
            rename_map[f.get('field')] = f.get('alias')
        for f in dependent_vars:
            rename_map[f.get('field')] = f.get('alias')
        
        # 对于 additional_fields 中的字段，如果没有 alias 定义，将保留原始字段名
        return result.rename(columns=rename_map)

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
    
    def export_split_sheets(self, df: pd.DataFrame, split_field: str, suffix: str = "") -> str:
        """
        将结果按指定字段拆分到多个 Excel Sheet 中导出
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
        
        # 使用 ExcelWriter 写入多个 Sheet
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # 获取拆分字段的所有唯一值（如不同的 Input Length 数值）
            unique_vals = sorted(df[split_field].unique())
            for val in unique_vals:
                # 1. 过滤当前值的数据
                # 2. 剔除作为 Sheet 名称的那个字段列
                sheet_df = df[df[split_field] == val].drop(columns=[split_field])
                
                # 写入 Sheet，名称为字段值（如 "4096", "8192" 等）
                sheet_name = str(val)[:31]  # Excel Sheet 名称限制 31 字符
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"结果已按 '{split_field}' 拆分并导出至: {output_path}")
        return output_path

    def run(self) -> Tuple[str, ...]:
        """
        执行完整处理流程
        """
        print("=" * 50)
        print("推理数据整合处理开始")
        print("=" * 50)
        
        # 1. 加载与预处理
        self.load_data()
        self.preprocess()
        
        # 2. 获取配置
        analysis_config = self.config.get('analysis', {})
        analysis_mode = analysis_config.get('mode', 'pivot')
        iv_config = self.config.get('independent_variables', {})
        output_config = self.config.get('output', {})
        split_by_npu = output_config.get('split_by_npu', False)
        
        output_files = []
        
        # 3. 处理透视表
        if analysis_mode in ('pivot', 'both', 'split_pivot'):
            print("  构建透视表...")
            pivot_df = self.build_pivot_table() # 包含之前修复的所有逻辑
            
            # 按卡数拆分输出
            if split_by_npu and 'Metric' in pivot_df.columns:
                print("  按卡数拆分输出...")
                # 单卡数据
                single_mask = pivot_df['Metric'] == '单卡'
                if single_mask.any():
                    single_df = pivot_df[single_mask].copy()
                    output_files.append(self.export(single_df, '单卡'))
                
                # 多卡数据（非单卡行）
                multi_mask = ~single_mask
                if multi_mask.any():
                    multi_df = pivot_df[multi_mask].copy()
                    output_files.append(self.export(multi_df, '多卡'))
                
                # 全量数据
                output_files.append(self.export(pivot_df, '全量'))
            elif analysis_mode == 'split_pivot':
                # 确定配置文件 row_fields 中定义的最后一个字段的 Alias
                row_fields_cfg = iv_config.get('row_fields', [])
                if row_fields_cfg:
                    last_cfg = row_fields_cfg[-1]
                    # 优先使用 alias，如果没有则使用 field
                    split_field_name = last_cfg.get('alias', last_cfg.get('field'))
                    
                    if split_field_name in pivot_df.columns:
                        path = self.export_split_sheets(pivot_df, split_field_name, 'split')
                        output_files.append(path)
                    else:
                        print(f"警告: 结果中未找到拆分字段 '{split_field_name}'，执行标准导出")
                        output_files.append(self.export(pivot_df, 'pivot'))
                else:
                    output_files.append(self.export(pivot_df, 'pivot'))
            else:
                # 标准导出
                output_files.append(self.export(pivot_df, 'pivot' if analysis_mode == 'both' else ''))
        
        # 4. 处理扁平表
        if analysis_mode in ('flat', 'both'):
            print("  构建扁平表...")
            flat_df = self.build_flat_table()
            output_files.append(self.export(flat_df, 'flat' if analysis_mode == 'both' else ''))
        
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