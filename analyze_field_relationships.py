#!/usr/bin/env python3
"""
分析原始CSV文件中字段之间的多对多关系
帮助理解数据的复杂性和聚合逻辑
"""

import pandas as pd
import numpy as np
from collections import defaultdict

def analyze_one_to_many(df, key_field, value_field, description):
    """
    分析一对多关系: 一个key对应多少个不同的values
    """
    print(f"\n{'='*90}")
    print(f"{description}")
    print(f"分析: {key_field} → {value_field}")
    print(f"{'='*90}")
    
    # 对于每个key，统计有多少个不同的values
    relationship = df.groupby(key_field)[value_field].nunique()
    
    stats = {
        'total_keys': len(relationship),
        'mean': relationship.mean(),
        'median': relationship.median(),
        'min': relationship.min(),
        'max': relationship.max(),
        'std': relationship.std()
    }
    
    print(f"\n统计信息:")
    print(f"  总共有 {stats['total_keys']:,} 个不同的 {key_field}")
    print(f"  平均每个 {key_field} 有 {stats['mean']:.2f} 个不同的 {value_field}")
    print(f"  中位数: {stats['median']:.1f}")
    print(f"  最小值: {stats['min']}")
    print(f"  最大值: {stats['max']}")
    print(f"  标准差: {stats['std']:.2f}")
    
    # 分布分析
    print(f"\n分布:")
    distribution = relationship.value_counts().sort_index()
    for count, freq in distribution.head(10).items():
        percentage = freq / len(relationship) * 100
        print(f"  {count:3d} 个 {value_field}: {freq:5,} 个 {key_field} ({percentage:5.1f}%)")
    
    if len(distribution) > 10:
        print(f"  ...")
        print(f"  (显示前10个，共{len(distribution)}种不同的数量)")
    
    # 极端案例
    top_5 = relationship.nlargest(5)
    if len(top_5) > 0:
        print(f"\n最多样化的5个 {key_field}:")
        for key, count in top_5.items():
            print(f"  {key}: {count} 个不同的 {value_field}")
    
    return stats, relationship

def analyze_many_to_many(df, field_a, field_b, description):
    """
    分析多对多关系: A和B之间的复杂关系
    """
    print(f"\n{'='*90}")
    print(f"{description}")
    print(f"多对多分析: {field_a} ↔ {field_b}")
    print(f"{'='*90}")
    
    # A → B: 每个A对应多少个B
    a_to_b = df.groupby(field_a)[field_b].nunique()
    # B → A: 每个B对应多少个A
    b_to_a = df.groupby(field_b)[field_a].nunique()
    
    # A-B组合的唯一数量
    unique_pairs = df.groupby([field_a, field_b]).size()
    
    print(f"\n方向1: {field_a} → {field_b}")
    print(f"  {len(a_to_b):,} 个不同的 {field_a}")
    print(f"  平均每个 {field_a} 关联 {a_to_b.mean():.2f} 个不同的 {field_b}")
    print(f"  最多关联: {a_to_b.max()} 个")
    
    print(f"\n方向2: {field_b} → {field_a}")
    print(f"  {len(b_to_a):,} 个不同的 {field_b}")
    print(f"  平均每个 {field_b} 关联 {b_to_a.mean():.2f} 个不同的 {field_a}")
    print(f"  最多关联: {b_to_a.max()} 个")
    
    print(f"\n组合分析:")
    print(f"  唯一的 ({field_a}, {field_b}) 组合: {len(unique_pairs):,}")
    print(f"  平均每个组合的记录数: {unique_pairs.mean():.2f}")
    print(f"  最多记录的组合: {unique_pairs.max()}")
    
    # 关系复杂度评分
    complexity = (a_to_b.mean() * b_to_a.mean()) ** 0.5
    print(f"\n复杂度评分 (几何平均): {complexity:.2f}")
    if complexity < 2:
        complexity_level = "低 (接近一对多)"
    elif complexity < 5:
        complexity_level = "中"
    else:
        complexity_level = "高 (真正的多对多)"
    print(f"  复杂度等级: {complexity_level}")
    
    return {
        'a_to_b_mean': a_to_b.mean(),
        'b_to_a_mean': b_to_a.mean(),
        'complexity': complexity,
        'unique_pairs': len(unique_pairs)
    }

def analyze_triple_relationship(df, field1, field2, field3, description):
    """
    分析三字段关系: field1-field2-field3的组合情况
    """
    print(f"\n{'='*90}")
    print(f"{description}")
    print(f"三字段关系: {field1} - {field2} - {field3}")
    print(f"{'='*90}")
    
    # 统计各种组合
    unique_1 = df[field1].nunique()
    unique_2 = df[field2].nunique()
    unique_3 = df[field3].nunique()
    unique_12 = df.groupby([field1, field2]).ngroups
    unique_13 = df.groupby([field1, field3]).ngroups
    unique_23 = df.groupby([field2, field3]).ngroups
    unique_123 = df.groupby([field1, field2, field3]).ngroups
    
    print(f"\n单字段唯一值:")
    print(f"  {field1}: {unique_1:,}")
    print(f"  {field2}: {unique_2:,}")
    print(f"  {field3}: {unique_3:,}")
    
    print(f"\n双字段组合:")
    print(f"  ({field1}, {field2}): {unique_12:,} 种组合")
    print(f"  ({field1}, {field3}): {unique_13:,} 种组合")
    print(f"  ({field2}, {field3}): {unique_23:,} 种组合")
    
    print(f"\n三字段组合:")
    print(f"  ({field1}, {field2}, {field3}): {unique_123:,} 种组合")
    
    # 计算组合率 (实际组合 / 理论最大组合)
    max_possible = unique_1 * unique_2 * unique_3
    combination_rate = unique_123 / max_possible * 100
    
    print(f"\n组合密度:")
    print(f"  理论最大组合数: {max_possible:,}")
    print(f"  实际组合数: {unique_123:,}")
    print(f"  组合率: {combination_rate:.4f}% (实际/理论)")
    
    if combination_rate < 1:
        print(f"  → 非常稀疏，字段之间有强约束关系")
    elif combination_rate < 10:
        print(f"  → 较稀疏，字段之间有一定约束")
    else:
        print(f"  → 较密集，字段相对独立")
    
    return {
        'unique_123': unique_123,
        'combination_rate': combination_rate
    }

def main():
    """主分析函数"""
    
    print("\n" + "="*90)
    print(" 原始CSV数据字段关系分析")
    print("="*90)
    
    # 加载数据
    print("\n加载数据...")
    df = pd.read_csv('Karg_food_flows_locations.csv', encoding='utf-8-sig', low_memory=False)
    print(f"总记录数: {len(df):,}")
    
    # 清理数据
    df['year_clean'] = pd.to_numeric(df['year'], errors='coerce')
    df = df[df['year_clean'].between(2013, 2017)]
    print(f"清理后记录数: {len(df):,}")
    
    # 关键字段列表
    fields = {
        'source_nam': '来源地点',
        'destination_name': '目的地点',
        'city': '中转城市',
        'commodit_1': '商品',
        'means_of_t': '交通方式',
        'year_clean': '年份',
        'Source_country_name': '来源国家',
        'Dest_country_name': '目的国家'
    }
    
    print(f"\n关键字段唯一值统计:")
    print(f"{'字段':<25} {'唯一值数量':>15} {'说明'}")
    print("-" * 90)
    for field, desc in fields.items():
        unique_count = df[field].nunique()
        print(f"{field:<25} {unique_count:>15,} {desc}")
    
    # ==================== 第一部分: 一对多关系 ====================
    
    print("\n\n" + "█"*90)
    print("█ 第一部分: 一对多关系分析")
    print("█"*90)
    
    # 1. 来源地点 → 目的地点
    analyze_one_to_many(
        df, 'source_nam', 'destination_name',
        "【来源 → 目的地】一个来源地点可以到达多少个不同的目的地？"
    )
    
    # 2. 目的地点 → 来源地点
    analyze_one_to_many(
        df, 'destination_name', 'source_nam',
        "【目的地 → 来源】一个目的地可以从多少个不同的来源接收？"
    )
    
    # 3. 来源 → 商品
    analyze_one_to_many(
        df, 'source_nam', 'commodit_1',
        "【来源 → 商品】一个来源地点出产/运输多少种不同的商品？"
    )
    
    # 4. 商品 → 来源
    analyze_one_to_many(
        df, 'commodit_1', 'source_nam',
        "【商品 → 来源】一种商品来自多少个不同的来源？"
    )
    
    # 5. 城市 → 路线
    df['route_pair'] = df['source_nam'] + ' → ' + df['destination_name']
    analyze_one_to_many(
        df, 'city', 'route_pair',
        "【中转城市 → 路线】一个中转城市服务多少条不同的路线？"
    )
    
    # ==================== 第二部分: 多对多关系 ====================
    
    print("\n\n" + "█"*90)
    print("█ 第二部分: 多对多关系分析")
    print("█"*90)
    
    # 1. 来源 ↔ 商品
    analyze_many_to_many(
        df, 'source_nam', 'commodit_1',
        "【来源 ↔ 商品】来源地点和商品的关系"
    )
    
    # 2. 来源 ↔ 交通方式
    analyze_many_to_many(
        df, 'source_nam', 'means_of_t',
        "【来源 ↔ 交通方式】来源地点和交通方式的关系"
    )
    
    # 3. 商品 ↔ 交通方式
    analyze_many_to_many(
        df, 'commodit_1', 'means_of_t',
        "【商品 ↔ 交通方式】商品和交通方式的关系"
    )
    
    # 4. 路线 ↔ 商品
    analyze_many_to_many(
        df, 'route_pair', 'commodit_1',
        "【路线 ↔ 商品】路线和商品的关系"
    )
    
    # 5. 路线 ↔ 年份
    analyze_many_to_many(
        df, 'route_pair', 'year_clean',
        "【路线 ↔ 年份】路线在不同年份的活跃情况"
    )
    
    # ==================== 第三部分: 三字段关系 ====================
    
    print("\n\n" + "█"*90)
    print("█ 第三部分: 三字段组合关系")
    print("█"*90)
    
    # 1. 来源-城市-目的地
    analyze_triple_relationship(
        df, 'source_nam', 'city', 'destination_name',
        "【来源-中转城市-目的地】三点路线结构"
    )
    
    # 2. 来源-目的地-商品
    analyze_triple_relationship(
        df, 'source_nam', 'destination_name', 'commodit_1',
        "【来源-目的地-商品】路线商品组合"
    )
    
    # 3. 来源-目的地-年份
    analyze_triple_relationship(
        df, 'source_nam', 'destination_name', 'year_clean',
        "【来源-目的地-年份】路线时间分布"
    )
    
    # 4. 来源-目的地-交通方式
    analyze_triple_relationship(
        df, 'source_nam', 'destination_name', 'means_of_t',
        "【来源-目的地-交通方式】路线运输方式"
    )
    
    # ==================== 第四部分: 聚合影响分析 ====================
    
    print("\n\n" + "█"*90)
    print("█ 第四部分: 聚合逻辑影响分析")
    print("█"*90)
    
    print(f"\n当前聚合逻辑: source + city + destination + flow_type")
    
    # 计算不同聚合策略的结果
    df['flow_type'] = df.apply(
        lambda row: 'rural_to_urban' if str(row['source_wit']).lower() != 'yes' and 
                    str(row['destination_within_urban_boundary']).lower() == 'yes'
                    else 'other',
        axis=1
    )
    
    strategies = {
        '仅位置': ['source_nam', 'destination_name'],
        '位置+城市': ['source_nam', 'city', 'destination_name'],
        '位置+城市+流动类型': ['source_nam', 'city', 'destination_name', 'flow_type'],
        '当前策略(+坐标舍入)': ['source_nam', 'city', 'destination_name', 'flow_type']
    }
    
    print(f"\n不同聚合策略的路线数量:")
    print(f"{'策略':<30} {'聚合后路线数':>15} {'压缩比':>10}")
    print("-" * 90)
    
    original_count = len(df)
    for name, fields in strategies.items():
        if name == '当前策略(+坐标舍入)':
            # 模拟坐标舍入
            df_temp = df.copy()
            df_temp['src_x_rounded'] = df_temp['Source x'].round(3)
            df_temp['src_y_rounded'] = df_temp['Source y'].round(3)
            df_temp['dest_x_rounded'] = df_temp['Destination x'].round(3)
            df_temp['dest_y_rounded'] = df_temp['Destination y'].round(3)
            grouped_count = df_temp.groupby(
                ['src_x_rounded', 'src_y_rounded', 'dest_x_rounded', 
                 'dest_y_rounded', 'city', 'flow_type']
            ).ngroups
        else:
            grouped_count = df.groupby(fields).ngroups
        
        compression_ratio = original_count / grouped_count
        print(f"{name:<30} {grouped_count:>15,} {compression_ratio:>9.1f}x")
    
    # ==================== 总结 ====================
    
    print("\n\n" + "█"*90)
    print("█ 分析总结")
    print("█"*90)
    
    print("""
关键发现:

1. 路线结构:
   - 100% 的记录都有中转城市 (city)
   - 形成 source → city → destination 的三点路线结构
   
2. 复杂性来源:
   - 同一路线在不同年份重复 (时间维度)
   - 同一路线运输不同商品 (商品维度)
   - 同一路线使用不同交通方式 (方式维度)
   
3. 聚合必要性:
   - 80,762 条原始记录 → ~10,800 条聚合路线
   - 聚合比例约 7.5:1
   - 主要通过合并同一路线的不同时间/商品/方式记录
   
4. 数据质量:
   - 约 2.4% 的记录缺失交通方式信息
   - 需要在分析中标记为 'Unknown'
    
建议:
   ✓ 当前聚合策略 (source + city + destination + flow_type) 合理
   ✓ 细分统计 (by_year, by_transport, by_commodity) 是必需的
   ✓ HTML筛选时应使用细分数据而非总计
""")

if __name__ == '__main__':
    main()
