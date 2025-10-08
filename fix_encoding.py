#!/usr/bin/env python3
"""
修复CSV中的编码错误
主要问题：√© 应该是 é (e with acute accent)
"""

import pandas as pd
import re

def fix_encoding_errors(text):
    """修复常见的编码错误 - UTF-8被误解析为Latin-1/Windows-1252"""
    if pd.isna(text):
        return text
    
    text = str(text)
    
    # 修复编码错误映射表
    # 原理：UTF-8的多字节序列被错误地按单字节编码解析
    # 例如：UTF-8的 é (C3 A9) 被当作两个Latin-1字符 √© 
    replacements = {
        # === 最常见的组合 (√ + 其他字符) ===
        '√©': 'é',   # e with acute - 30,729次
        '√¥': 'ô',   # o with circumflex - 832次
        '√®': 'è',   # e with grave - 194次
        '√¢': 'â',   # a with circumflex
        '√Ø': 'ô',   # o with circumflex (另一种编码)
        '√™': 'ê',   # e with circumflex
        '√†': 'à',   # a with grave
        '√Æ': 'î',   # i with circumflex
        '√π': 'ù',   # u with grave
        '√ª': 'û',   # u with circumflex
        '√ß': 'ç',   # c with cedilla
        '√â': 'É',   # E with acute
        '√Ä': 'À',   # A with grave
        '√´': 'ú',   # u with acute
        '√≥': 'ó',   # o with acute
        '√≠': 'í',   # i with acute
        '√±': 'ñ',   # n with tilde
        
        # === 单独的特殊符号（可能残留） ===
        '¢': 'â',    # CENT SIGN - 可能是â的一部分
        '¥': 'ô',    # YEN SIGN - 可能是ô的一部分
        '®': 'è',    # REGISTERED SIGN - 可能是è的一部分
        '©': 'é',    # COPYRIGHT SIGN - 可能是é的一部分
        '≠': 'é',    # NOT EQUAL TO - 可能是é的一部分
        
        # === 其他可能的组合 ===
        '¬©': 'é',   # 另一种编码错误
        '¬´': 'ó',
        '¬≠': 'í',
    }
    
    # 按长度排序，先替换长的（避免部分替换问题）
    for wrong, correct in sorted(replacements.items(), key=lambda x: len(x[0]), reverse=True):
        text = text.replace(wrong, correct)
    
    # 特殊处理：Ø 字符（挪威/丹麦语的合法字符，但在某些情况下是错误编码）
    # 只在特定上下文中替换（如 "Dio√Øla" → "Dioîla"）
    if 'Dio√Øla' in text:
        text = text.replace('Dio√Øla', 'Dioïla')
    
    return text

def main():
    print("=" * 60)
    print("🔧 编码错误修复工具")
    print("=" * 60)
    print()
    
    # 读取CSV
    print("📖 读取数据: Karg_food_flows_locations.csv")
    df = pd.read_csv('Karg_food_flows_locations.csv', low_memory=False)
    print(f"   ✓ 总记录数: {len(df):,}")
    print()
    
    # 统计修复前的问题
    all_locations_before = set(list(df['source_nam'].dropna()) + list(df['destination_name'].dropna()))
    
    # 检测多种编码错误标记
    error_markers = ['√', '¢', '¥', '©', '®', '≠']
    problem_locations_before = [
        loc for loc in all_locations_before 
        if any(marker in str(loc) for marker in error_markers)
    ]
    
    print(f"📊 修复前统计:")
    print(f"   - 唯一位置数: {len(all_locations_before):,}")
    print(f"   - 有编码问题: {len(problem_locations_before):,} ({len(problem_locations_before)/len(all_locations_before)*100:.1f}%)")
    print()
    
    # 按错误类型统计
    print("📋 错误类型统计:")
    for marker in error_markers:
        count = sum(1 for loc in problem_locations_before if marker in str(loc))
        if count > 0:
            print(f"   - 包含 '{marker}': {count:,}")
    print()
    
    # 显示一些例子
    print("🔍 编码错误示例 (前15个):")
    for i, loc in enumerate(problem_locations_before[:15]):
        fixed = fix_encoding_errors(loc)
        if loc != fixed:  # 只显示有变化的
            print(f"   {i+1:2}. \"{loc}\"")
            print(f"       → \"{fixed}\"")
    print()
    
    # 修复所有文本列（自动检测所有object类型的列）
    text_columns = [col for col in df.columns if df[col].dtype == 'object']
    
    print(f"✨ 修复数据列 (共{len(text_columns)}列):")
    total_before = 0
    total_after = 0
    
    for col in text_columns:
        before_count = df[col].astype(str).str.contains('|'.join(error_markers), regex=True, na=False).sum()
        if before_count > 0:  # 只处理有问题的列
            df[col] = df[col].apply(fix_encoding_errors)
            after_count = df[col].astype(str).str.contains('|'.join(error_markers), regex=True, na=False).sum()
            fixed = before_count - after_count
            total_before += before_count
            total_after += after_count
            print(f"   - {col:25} : {before_count:5} → {after_count:5} ({fixed} 修复)")
    
    print()
    print(f"   总计: {total_before:,} → {total_after:,} ({total_before - total_after:,} 修复)")
    print()
    
    # 统计修复后的问题
    all_locations_after = set(list(df['source_nam'].dropna()) + list(df['destination_name'].dropna()))
    problem_locations_after = [
        loc for loc in all_locations_after 
        if any(marker in str(loc) for marker in error_markers)
    ]
    
    print(f"📊 修复后统计:")
    print(f"   - 唯一位置数: {len(all_locations_after):,}")
    print(f"   - 仍有编码问题: {len(problem_locations_after):,}")
    print(f"   - 成功修复: {len(problem_locations_before) - len(problem_locations_after):,} ({(len(problem_locations_before) - len(problem_locations_after))/len(problem_locations_before)*100:.1f}%)")
    print()
    
    # 显示未修复的问题（如果有）
    if len(problem_locations_after) > 0:
        print("⚠️ 仍有编码问题的位置:")
        for i, loc in enumerate(problem_locations_after[:10]):
            print(f"   {i+1}. \"{loc}\"")
        if len(problem_locations_after) > 10:
            print(f"   ... 还有 {len(problem_locations_after) - 10} 个")
        print()
    
    # 保存修复后的文件
    output_file = 'Karg_food_flows_locations_fixed.csv'
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"✅ 已保存到: {output_file}")
    print()
    
    # 显示修复后的示例
    print("✨ 修复示例 (前10个):")
    for i, loc_before in enumerate(problem_locations_before[:10]):
        loc_after = fix_encoding_errors(loc_before)
        if loc_before != loc_after:
            print(f"   {i+1:2}. {loc_after}")
    
    print()
    print("=" * 60)
    print("🎉 修复完成！")
    print("=" * 60)

if __name__ == '__main__':
    main()

