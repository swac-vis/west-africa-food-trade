#!/usr/bin/env python3
"""
Enhanced food flows processing with Rural-Urban analysis
Adds classification of flows by rural/urban patterns
Now with OSRM real route paths
New hierarchical data structure by year

USAGE:
  Basic (no OSRM):          python process_rural_urban_analysis.py
  With OSRM (all routes):   python process_rural_urban_analysis.py --osrm
  With OSRM (top 100):      python process_rural_urban_analysis.py --osrm --top100

CRASH RECOVERY:
  - OSRM cache is saved every 50 routes to 'osrm_route_cache.json'
  - Progress is tracked in 'osrm_progress.json'
  - Intermediate results saved to '*_temp.json'
  - If interrupted (Ctrl+C or crash), simply run the same command again
  - Script will ask if you want to resume from where it stopped
  
FEATURES:
  - ✓ Automatic resume from last checkpoint
  - ✓ Progress tracking every 50 routes
  - ✓ Safe Ctrl+C interrupt (saves before exit)
  - ✓ Crash recovery with intermediate files
  - ✓ OSRM route caching (no duplicate API calls)
"""

import pandas as pd
import json
import requests
import time
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

# OSRM路径缓存
ROUTE_CACHE = {}
CACHE_FILE = 'osrm_route_cache_round1.json'  # round1 专用缓存
PROGRESS_FILE = 'osrm_progress_round1.json'  # round1 专用进度

def load_route_cache():
    """加载OSRM路径缓存"""
    global ROUTE_CACHE
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            ROUTE_CACHE = json.load(f)
        print(f"✓ Loaded {len(ROUTE_CACHE)} cached routes")
    return ROUTE_CACHE

def save_route_cache():
    """保存OSRM路径缓存"""
    with open(CACHE_FILE, 'w') as f:
        json.dump(ROUTE_CACHE, f, indent=2)
    print(f"💾 Saved {len(ROUTE_CACHE)} routes to cache")

def load_progress():
    """加载处理进度"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
        print(f"✓ Found previous progress: {progress['processed']}/{progress['total']} routes processed")
        return progress
    return None

def save_progress(processed, total, last_route_id):
    """保存处理进度"""
    progress = {
        'processed': processed,
        'total': total,
        'last_route_id': last_route_id,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def save_intermediate_results(data, filename='food_flows_by_year_temp.json'):
    """保存中间结果（防止数据丢失）"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved intermediate results to {filename}")

def simplify_path(points, epsilon=0.001):
    """
    使用 Douglas-Peucker 算法简化路径
    epsilon: 简化阈值（度数），0.001 约等于 111 米
    """
    if len(points) < 3:
        return points
    
    def perpendicular_distance(point, line_start, line_end):
        """计算点到线段的垂直距离"""
        x0, y0 = point
        x1, y1 = line_start
        x2, y2 = line_end
        
        # 线段长度
        dx = x2 - x1
        dy = y2 - y1
        
        if dx == 0 and dy == 0:
            return ((x0 - x1)**2 + (y0 - y1)**2)**0.5
        
        # 点到线的距离
        t = max(0, min(1, ((x0 - x1) * dx + (y0 - y1) * dy) / (dx * dx + dy * dy)))
        proj_x = x1 + t * dx
        proj_y = y1 + t * dy
        
        return ((x0 - proj_x)**2 + (y0 - proj_y)**2)**0.5
    
    def rdp(points, epsilon):
        """递归 Douglas-Peucker 算法"""
        if len(points) < 3:
            return points
        
        # 找到距离起点-终点连线最远的点
        max_dist = 0
        max_index = 0
        
        for i in range(1, len(points) - 1):
            dist = perpendicular_distance(points[i], points[0], points[-1])
            if dist > max_dist:
                max_dist = dist
                max_index = i
        
        # 如果最大距离大于阈值，递归简化
        if max_dist > epsilon:
            left = rdp(points[:max_index + 1], epsilon)
            right = rdp(points[max_index:], epsilon)
            return left[:-1] + right
        else:
            return [points[0], points[-1]]
    
    simplified = rdp(points, epsilon)
    return simplified

def print_progress_bar(current, total, success, fail, start_time, prefix='Progress'):
    """
    打印进度条和统计信息
    """
    # 计算百分比
    percent = 100 * (current / float(total))
    filled_length = int(50 * current // total)
    bar = '█' * filled_length + '░' * (50 - filled_length)
    
    # 计算速度和ETA
    elapsed = time.time() - start_time
    if current > 0:
        rate = current / elapsed
        eta_seconds = (total - current) / rate if rate > 0 else 0
        eta_str = str(timedelta(seconds=int(eta_seconds)))
        speed_str = f"{rate:.1f} routes/s"
    else:
        eta_str = "calculating..."
        speed_str = "0 routes/s"
    
    # 统计信息
    stats = f"✓{success} ✗{fail}"
    
    # 打印进度条（使用 \r 覆盖同一行）
    sys.stdout.write(f'\r   {prefix}: |{bar}| {percent:.1f}% ({current}/{total}) {stats} | Speed: {speed_str} | ETA: {eta_str}')
    sys.stdout.flush()

def get_osrm_route(source_coords, via_coords, dest_coords, route_id):
    """
    获取经过三点的OSRM路径
    
    Args:
        source_coords: [lon, lat] 起点
        via_coords: [lon, lat] 经过点
        dest_coords: [lon, lat] 终点
        route_id: 路线ID用于缓存
    
    Returns:
        {
            'path': [[lon, lat], ...],  # 路径坐标
            'distance_km': float,        # 距离（公里）
            'duration_hours': float      # 时长（小时）
        }
        或 None（失败时）
    """
    # 检查缓存
    cache_key = f"{route_id}"
    if cache_key in ROUTE_CACHE:
        return ROUTE_CACHE[cache_key]
    
    try:
        # 构建OSRM请求
        coords_str = f"{source_coords[0]},{source_coords[1]};{via_coords[0]},{via_coords[1]};{dest_coords[0]},{dest_coords[1]}"
        url = f"http://router.project-osrm.org/route/v1/driving/{coords_str}"
        params = {
            'overview': 'full',
            'geometries': 'geojson'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.ok:
            data = response.json()
            if data.get('routes'):
                route = data['routes'][0]
                result = {
                    'path': route['geometry']['coordinates'],
                    'distance_km': route['distance'] / 1000,  # 米转公里
                    'duration_hours': route['duration'] / 3600  # 秒转小时
                }
                
                # 缓存结果
                ROUTE_CACHE[cache_key] = result
                return result
        
        print(f"  ⚠️  OSRM failed for route {route_id}: {response.status_code}")
        return None
        
    except Exception as e:
        print(f"  ❌ Error getting OSRM route {route_id}: {e}")
        return None

def load_and_clean_data(csv_path):
    """Load and clean the full dataset"""
    print("Loading full dataset...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
    
    # Clean data
    df['year_clean'] = pd.to_numeric(df['year'], errors='coerce')
    df = df[df['year_clean'].between(2013, 2017)]
    
    for col in ['Source x', 'Source y', 'Destination x', 'Destination y']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=['Source x', 'Source y', 'Destination x', 'Destination y'])
    
    print(f"Loaded {len(df)} records")
    return df

def classify_flow_type(source_urban, dest_urban):
    """
    Classify flow type based on rural/urban status
    """
    source_is_urban = str(source_urban).lower() == 'yes'
    dest_is_urban = str(dest_urban).lower() == 'yes'
    
    if source_is_urban and dest_is_urban:
        return 'urban_to_urban'
    elif source_is_urban and not dest_is_urban:
        return 'urban_to_rural'
    elif not source_is_urban and dest_is_urban:
        return 'rural_to_urban'
    else:
        return 'rural_to_rural'

def get_flow_type_label(flow_type):
    """Get human-readable label"""
    labels = {
        'rural_to_urban': 'Rural → Urban',
        'urban_to_rural': 'Urban → Rural',
        'rural_to_rural': 'Rural → Rural',
        'urban_to_urban': 'Urban → Urban'
    }
    return labels.get(flow_type, 'Unknown')

def analyze_rural_urban_patterns(df):
    """Analyze rural-urban flow patterns"""
    print("\nAnalyzing rural-urban patterns...")
    
    # Add flow type classification
    df['flow_type'] = df.apply(
        lambda row: classify_flow_type(
            row['source_wit'], 
            row['destination_within_urban_boundary']
        ), 
        axis=1
    )
    
    # Overall statistics
    flow_type_counts = df['flow_type'].value_counts().to_dict()
    
    analysis = {
        'total_flows': len(df),
        'flow_patterns': {}
    }
    
    for flow_type, count in flow_type_counts.items():
        flow_df = df[df['flow_type'] == flow_type]
        
        analysis['flow_patterns'][flow_type] = {
            'label': get_flow_type_label(flow_type),
            'count': int(count),
            'percentage': round(count / len(df) * 100, 2),
            'total_quantity': float(flow_df['total_quantity'].sum()) if not flow_df['total_quantity'].isna().all() else 0,
            'avg_distance': float(flow_df['distance_1'].mean()) if 'distance_1' in flow_df else 0,
            'international_count': int(len(flow_df[flow_df['Crosses international border?'] == 'YES'])),
            'top_commodities': flow_df['commodit_1'].value_counts().head(5).to_dict()
        }
    
    return analysis, df


def build_city_coordinates_lookup(df):
    """
    Build a lookup table for city coordinates from source and destination data
    """
    print("Building city coordinates lookup table...")
    city_coords = {}
    
    # Get unique cities
    unique_cities = df['city'].dropna().unique()
    
    for city in unique_cities:
        # Try to find coordinates from source locations
        source_matches = df[df['source_nam'].str.contains(city, na=False, case=False)]
        if len(source_matches) > 0:
            # Use median coordinates for robustness
            coords = [
                float(source_matches['Source x'].median()),
                float(source_matches['Source y'].median())
            ]
            city_coords[city] = coords
            continue
        
        # Try to find coordinates from destination locations
        dest_matches = df[df['destination_name'].str.contains(city, na=False, case=False)]
        if len(dest_matches) > 0:
            coords = [
                float(dest_matches['Destination x'].median()),
                float(dest_matches['Destination y'].median())
            ]
            city_coords[city] = coords
    
    print(f"Found coordinates for {len(city_coords)} cities")
    return city_coords

def create_routes_with_rural_urban(df, min_flows=1):
    """
    Create aggregated routes with rural-urban classification
    Includes via_city information for three-point routes (source -> city -> destination)
    """
    print(f"Creating routes with rural-urban info (min {min_flows} flows)...")
    
    # Build city coordinates lookup
    city_coords_lookup = build_city_coordinates_lookup(df)
    
    routes = []
    
    # === 按三点路线聚合: 以坐标为准（名字可重复）===
    # 坐标舍入到小数点后1位（约11公里精度），聚合附近的点
    
    # Add city to grouping (using fillna to handle missing values)
    df['city_grouped'] = df['city'].fillna('direct')
    
    # Round coordinates for aggregation (1 decimal place ≈ 11km precision)
    # 更激进的聚合，减少更多路线
    df['src_x_rounded'] = df['Source x'].round(1)
    df['src_y_rounded'] = df['Source y'].round(1)
    df['dest_x_rounded'] = df['Destination x'].round(1)
    df['dest_y_rounded'] = df['Destination y'].round(1)
    
    grouped = df.groupby([
        'src_x_rounded',
        'src_y_rounded',
        'dest_x_rounded',
        'dest_y_rounded',
        'city_grouped',
        'flow_type'
    ])
    
    for key, route_df in grouped:
        src_x_rounded, src_y_rounded, dest_x_rounded, dest_y_rounded, city_name, flow_type = key
        
        if len(route_df) < min_flows:
            continue
        
        # 使用舍入后的坐标（作为唯一标识）
        src_x = src_x_rounded
        src_y = src_y_rounded
        dest_x = dest_x_rounded
        dest_y = dest_y_rounded
        
        # 获取名字（取众数，因为同一坐标可能有多个名字）
        source_name = route_df['source_nam'].mode()[0] if len(route_df['source_nam'].mode()) > 0 else 'Unknown'
        dest_name = route_df['destination_name'].mode()[0] if len(route_df['destination_name'].mode()) > 0 else 'Unknown'
        
        src_country = route_df['Source_country_name'].mode()[0] if len(route_df['Source_country_name'].mode()) > 0 else 'Unknown'
        dest_country = route_df['Dest_country_name'].mode()[0] if len(route_df['Dest_country_name'].mode()) > 0 else 'Unknown'
        
        total_flows = len(route_df)
        total_quantity = float(route_df['total_quantity'].sum()) if not route_df['total_quantity'].isna().all() else 0
        
        commodity_counts = route_df['commodit_1'].value_counts().head(5).to_dict()
        categories = route_df['commodit_2'].dropna().unique().tolist()
        years = sorted(route_df['year_clean'].dropna().unique().tolist())
        
        # Get transportation modes
        transport_modes = route_df['means_of_t'].value_counts().to_dict()
        main_transport = route_df['means_of_t'].mode()[0] if len(route_df['means_of_t'].mode()) > 0 else 'Unknown'
        
        # === NEW: Build detailed breakdowns by year and transport mode ===
        # By year breakdown
        by_year = {}
        for year in years:
            year_df = route_df[route_df['year_clean'] == year]
            year_commodities = year_df['commodit_1'].value_counts().to_dict()
            # Handle missing transport modes
            year_df_temp = year_df.copy()
            year_df_temp['means_of_t_clean'] = year_df_temp['means_of_t'].fillna('Unknown')
            year_transport = year_df_temp['means_of_t_clean'].value_counts().to_dict()
            by_year[int(year)] = {
                'flows': int(len(year_df)),
                'quantity': float(year_df['total_quantity'].sum()) if not year_df['total_quantity'].isna().all() else 0,
                'commodities': {str(k): int(v) for k, v in year_commodities.items()},
                'transport_modes': {str(k): int(v) for k, v in year_transport.items()}
            }
        
        # By transport mode breakdown (handle missing values)
        by_transport = {}
        # Create a temporary column with 'Unknown' for missing transport modes
        route_df_temp = route_df.copy()
        route_df_temp['means_of_t_clean'] = route_df_temp['means_of_t'].fillna('Unknown')
        
        # Get all transport modes including 'Unknown'
        all_transport_modes = route_df_temp['means_of_t_clean'].value_counts().to_dict()
        
        for transport in all_transport_modes.keys():
            trans_df = route_df_temp[route_df_temp['means_of_t_clean'] == transport]
            trans_years = sorted(trans_df['year_clean'].dropna().unique().tolist())
            trans_commodities = trans_df['commodit_1'].value_counts().to_dict()
            by_transport[str(transport)] = {
                'flows': int(len(trans_df)),
                'quantity': float(trans_df['total_quantity'].sum()) if not trans_df['total_quantity'].isna().all() else 0,
                'years': [int(y) for y in trans_years],
                'commodities': {str(k): int(v) for k, v in trans_commodities.items()}
            }
        
        # By commodity breakdown
        by_commodity = {}
        for commodity in commodity_counts.keys():
            comm_df = route_df[route_df['commodit_1'] == commodity]
            comm_years = sorted(comm_df['year_clean'].dropna().unique().tolist())
            # Handle missing transport modes
            comm_df_temp = comm_df.copy()
            comm_df_temp['means_of_t_clean'] = comm_df_temp['means_of_t'].fillna('Unknown')
            comm_transport = comm_df_temp['means_of_t_clean'].value_counts().to_dict()
            by_commodity[str(commodity)] = {
                'flows': int(len(comm_df)),
                'quantity': float(comm_df['total_quantity'].sum()) if not comm_df['total_quantity'].isna().all() else 0,
                'years': [int(y) for y in comm_years],
                'transport_modes': {str(k): int(v) for k, v in comm_transport.items()}
            }
        
        is_intl = (src_country != dest_country) if pd.notna(src_country) and pd.notna(dest_country) else False
        
        # Determine urban status
        source_is_urban = route_df['source_wit'].mode()[0] if len(route_df['source_wit'].mode()) > 0 else 'no'
        dest_is_urban = route_df['destination_within_urban_boundary'].mode()[0] if len(route_df['destination_within_urban_boundary'].mode()) > 0 else 'no'
        
        # Build route object (source_name和dest_name已经从groupby key中获取)
        route = {
            'source': {
                'name': str(source_name),
                'country': str(src_country) if pd.notna(src_country) else 'Unknown',
                'coordinates': [float(src_x), float(src_y)],
                'is_urban': str(source_is_urban).lower() == 'yes'
            },
            'destination': {
                'name': str(dest_name),
                'country': str(dest_country) if pd.notna(dest_country) else 'Unknown',
                'coordinates': [float(dest_x), float(dest_y)],
                'is_urban': str(dest_is_urban).lower() == 'yes'
            },
            'flows': int(total_flows),
            'quantity': float(total_quantity),
            'commodities': {k: int(v) for k, v in commodity_counts.items()},
            'categories': [str(c) for c in categories],
            'years': [int(y) for y in years],
            'transport_modes': {str(k): int(v) for k, v in transport_modes.items()},
            'main_transport': str(main_transport),
            'is_international': bool(is_intl),
            'flow_type': flow_type,
            'flow_type_label': get_flow_type_label(flow_type),
            # === NEW: Detailed breakdowns for precise filtering ===
            'by_year': by_year,
            'by_transport': by_transport,
            'by_commodity': by_commodity
        }
        
        # Add via_city information if route goes through a city
        if city_name != 'direct' and city_name in city_coords_lookup:
            route['via_city'] = {
                'name': str(city_name),
                'coordinates': city_coords_lookup[city_name]
            }
        
        routes.append(route)
    
    print(f"Created {len(routes)} routes")
    return routes


def create_hierarchical_data_by_year(df):
    """
    Create hierarchical data structure organized by year
    
    Structure:
    {
      "2013": {
        "route_id_1": {
          "source": {...},
          "destination": {...},
          "via_city": {...},
          "flow": total_flows,
          "quantity": total_quantity,
          "commodity": {
            "Maize": {
              "category": "Cereal",
              "quantity": quantity,
              "transport": {
                "Truck": {
                  "quantity": quantity,
                  "flow": flow_count
                }
              }
            }
          },
          "is_international": true/false,
          "flow_type": "urban-urban",
          # "path": [...] # Will be added later with OSRM
        }
      }
    }
    """
    print("Creating hierarchical data structure by year...")
    
    # Build city coordinates lookup
    city_coords_lookup = build_city_coordinates_lookup(df)
    
    # Round coordinates for aggregation (1 decimal place ≈ 11km precision)
    # 更激进的聚合，减少更多路线
    df['src_x_rounded'] = df['Source x'].round(1)
    df['src_y_rounded'] = df['Source y'].round(1)
    df['dest_x_rounded'] = df['Destination x'].round(1)
    df['dest_y_rounded'] = df['Destination y'].round(1)
    df['city_grouped'] = df['city'].fillna('direct')
    
    # Result structure: year -> route_id -> data
    data_by_year = defaultdict(dict)
    
    # Group by coordinates, city, flow_type, and year
    grouped = df.groupby([
        'src_x_rounded',
        'src_y_rounded',
        'dest_x_rounded',
        'dest_y_rounded',
        'city_grouped',
        'flow_type',
        'year_clean'
    ])
    
    for key, route_df in grouped:
        src_x, src_y, dest_x, dest_y, city_name, flow_type, year = key
        
        if pd.isna(year):
            continue
        
        year_str = str(int(year))
        
        # Create route_id based on coordinates
        route_id = f"r_{src_x}_{src_y}_{dest_x}_{dest_y}_{city_name}"
        
        # Get names (mode = most frequent)
        source_name = route_df['source_nam'].mode()[0] if len(route_df['source_nam'].mode()) > 0 else 'Unknown'
        dest_name = route_df['destination_name'].mode()[0] if len(route_df['destination_name'].mode()) > 0 else 'Unknown'
        src_country = route_df['Source_country_name'].mode()[0] if len(route_df['Source_country_name'].mode()) > 0 else 'Unknown'
        dest_country = route_df['Dest_country_name'].mode()[0] if len(route_df['Dest_country_name'].mode()) > 0 else 'Unknown'
        
        # Check if route_id already exists for this year (aggregate across all groups)
        if route_id not in data_by_year[year_str]:
            # Initialize route data
            data_by_year[year_str][route_id] = {
                'source': {
                    'name': source_name,
                    'coordinates': [float(src_x), float(src_y)],
                    'country': src_country,
                    'is_urban': str(route_df['source_wit'].mode()[0]).lower() == 'yes' if 'source_wit' in route_df.columns and len(route_df['source_wit'].mode()) > 0 else False
                },
                'destination': {
                    'name': dest_name,
                    'coordinates': [float(dest_x), float(dest_y)],
                    'country': dest_country,
                    'is_urban': str(route_df['destination_within_urban_boundary'].mode()[0]).lower() == 'yes' if 'destination_within_urban_boundary' in route_df.columns and len(route_df['destination_within_urban_boundary'].mode()) > 0 else False
                },
                'via_city': {
                    'name': city_name if city_name != 'direct' else None,
                    'coordinates': city_coords_lookup.get(city_name, [None, None]) if city_name != 'direct' else None
                },
                'flow': 0,
                'quantity': 0.0,
                'commodity': {},
                'is_international': src_country != dest_country,
                'flow_type': flow_type,
                # 'path': []  # Commented out for later OSRM processing
            }
        
        # Aggregate flows and quantity
        route_data = data_by_year[year_str][route_id]
        route_data['flow'] += len(route_df)
        route_data['quantity'] += float(route_df['total_quantity'].sum()) if not route_df['total_quantity'].isna().all() else 0
        
        # Process commodities
        for _, row in route_df.iterrows():
            commodity = row['commodit_1'] if pd.notna(row['commodit_1']) else 'Unknown'
            category = row['commodit_2'] if pd.notna(row['commodit_2']) else 'Unknown'
            transport = row['means_of_t'] if pd.notna(row['means_of_t']) else 'Unknown'
            quantity_val = float(row['total_quantity']) if pd.notna(row['total_quantity']) else 0
            
            # Initialize commodity if not exists
            if commodity not in route_data['commodity']:
                route_data['commodity'][commodity] = {
                    'category': category,
                    'quantity': 0.0,
                    'transport': {}
                }
            
            # Aggregate commodity quantity
            route_data['commodity'][commodity]['quantity'] += quantity_val
            
            # Initialize transport if not exists
            if transport not in route_data['commodity'][commodity]['transport']:
                route_data['commodity'][commodity]['transport'][transport] = {
                    'quantity': 0.0,
                    'flow': 0
                }
            
            # Aggregate transport data
            route_data['commodity'][commodity]['transport'][transport]['quantity'] += quantity_val
            route_data['commodity'][commodity]['transport'][transport]['flow'] += 1
    
    # Convert defaultdict to regular dict
    result = {year: dict(routes) for year, routes in data_by_year.items()}
    
    # Print summary
    total_routes = sum(len(routes) for routes in result.values())
    print(f"   Created {total_routes} routes across {len(result)} years")
    for year in sorted(result.keys()):
        print(f"     {year}: {len(result[year])} routes")
    
    return result


def main():
    """Main processing function"""
    import sys
    
    # Use the fixed CSV with corrected encoding
    csv_path = 'Karg_food_flows_locations_fixed.csv'
    use_osrm = '--osrm' in sys.argv
    skip_prompt = '--skip-prompt' in sys.argv or '--yes' in sys.argv or '-y' in sys.argv
    
    # Check if food_flows_by_year.json already exists
    hierarchical_file = 'food_flows_by_year_round1.json'  # round(1) = 11km精度
    temp_file = 'food_flows_by_year_round1_temp.json'
    
    # Priority 1: Check for existing hierarchical data (skip data processing)
    if os.path.exists(hierarchical_file) and use_osrm:
        print("\n" + "="*70)
        print("🔄 FOUND EXISTING DATA FILE")
        print("="*70)
        print(f"\n   Found {hierarchical_file}")
        
        if skip_prompt:
            response = 'y'
            print(f"   Auto-loading existing data (--skip-prompt)")
        else:
            response = input(f"   Load existing data and only update OSRM paths? (y/n): ")
        
        if response.lower() == 'y':
            print(f"   Loading existing hierarchical data...")
            with open(hierarchical_file, 'r', encoding='utf-8') as f:
                hierarchical_data = json.load(f)
            
            total_routes = sum(len(routes) for routes in hierarchical_data.values())
            print(f"   ✓ Loaded {total_routes} routes from {len(hierarchical_data)} years")
            
            # Convert hierarchical data back to flat all_routes (for OSRM processing)
            print(f"   Converting to flat route list...")
            all_routes = []
            for year_str, year_routes in hierarchical_data.items():
                for route_id, route_data in year_routes.items():
                    # Check if this route already exists in all_routes
                    existing = next((r for r in all_routes 
                                   if r['source']['coordinates'] == route_data['source']['coordinates'] 
                                   and r['destination']['coordinates'] == route_data['destination']['coordinates']
                                   and r.get('via_city', {}).get('name') == route_data.get('via_city', {}).get('name')), 
                                   None)
                    
                    if existing:
                        # Merge years
                        if int(year_str) not in existing['years']:
                            existing['years'].append(int(year_str))
                        # Copy path if exists
                        if 'path' in route_data and route_data['path']:
                            existing['path'] = route_data['path']
                            existing['distance_km'] = route_data.get('distance_km')
                            existing['duration_hours'] = route_data.get('duration_hours')
                    else:
                        # Create new route entry
                        new_route = {
                            'source': route_data['source'],
                            'destination': route_data['destination'],
                            'via_city': route_data.get('via_city'),
                            'flows': route_data['flow'],
                            'quantity': route_data['quantity'],
                            'is_international': route_data['is_international'],
                            'flow_type': route_data['flow_type'],
                            'years': [int(year_str)]
                        }
                        if 'path' in route_data and route_data['path']:
                            new_route['path'] = route_data['path']
                            new_route['distance_km'] = route_data.get('distance_km')
                            new_route['duration_hours'] = route_data.get('duration_hours')
                        all_routes.append(new_route)
            
            print(f"   ✓ Converted to {len(all_routes)} unique routes")
            
            # Create dummy analysis for summary
            overall_analysis = {'flow_patterns': {}}
            df_with_types = None
            
            print(f"   ✓ Ready to update OSRM paths (skipped CSV processing)\n")
            # Continue to OSRM section
        else:
            print(f"   Starting fresh analysis...")
            df = load_and_clean_data(csv_path)
            overall_analysis, df_with_types = analyze_rural_urban_patterns(df)
            all_routes = create_routes_with_rural_urban(df_with_types, min_flows=1)
            hierarchical_data = create_hierarchical_data_by_year(df_with_types)
    
    # Priority 2: Check for temporary file (resume from interruption)
    elif use_osrm and os.path.exists(temp_file):
        print("\n" + "="*70)
        print("🔄 RESUMING FROM PREVIOUS RUN")
        print("="*70)
        print(f"\n   Found temporary file: {temp_file}")
        
        if skip_prompt:
            response = 'y'
            print(f"   Auto-resuming from temporary file (--skip-prompt)")
        else:
            response = input(f"   Load partial results and continue? (y/n): ")
        
        if response.lower() == 'y':
            print(f"   Loading partial hierarchical data...")
            with open(temp_file, 'r', encoding='utf-8') as f:
                hierarchical_data = json.load(f)
            
            # Load original data and rebuild all_routes for OSRM processing
            df = load_and_clean_data(csv_path)
            overall_analysis, df_with_types = analyze_rural_urban_patterns(df)
            all_routes = create_routes_with_rural_urban(df_with_types, min_flows=1)
            
            total_routes = sum(len(routes) for routes in hierarchical_data.values())
            print(f"   ✓ Loaded hierarchical data ({total_routes} routes)")
            print(f"   Continuing OSRM processing...\n")
            
            # Skip to OSRM processing section
            import sys
            sys.argv.append('--resume')
            # Will be handled in OSRM section below
        else:
            print(f"   Starting fresh analysis...")
            df = load_and_clean_data(csv_path)
            print("\n" + "="*70)
            print("RURAL-URBAN FOOD FLOWS ANALYSIS")
            print("="*70 + "\n")
            overall_analysis, df_with_types = analyze_rural_urban_patterns(df)
            print("   Flow Pattern Distribution:")
            for flow_type, data in overall_analysis['flow_patterns'].items():
                print(f"   {data['label']:20s}: {data['count']:6,} ({data['percentage']:5.2f}%)")
            print("\n2. Creating Route Files")
            all_routes = create_routes_with_rural_urban(df_with_types, min_flows=1)
            hierarchical_data = create_hierarchical_data_by_year(df_with_types)
    else:
        # Normal flow - fresh start
        df = load_and_clean_data(csv_path)
        
        print("\n" + "="*70)
        print("RURAL-URBAN FOOD FLOWS ANALYSIS")
        print("="*70 + "\n")
        
        # Overall rural-urban pattern analysis (needed for flow_type classification)
        print("1. Analyzing rural-urban patterns...")
        overall_analysis, df_with_types = analyze_rural_urban_patterns(df)
        
        # Print summary
        print("   Flow Pattern Distribution:")
        for flow_type, data in overall_analysis['flow_patterns'].items():
            print(f"   {data['label']:20s}: {data['count']:6,} ({data['percentage']:5.2f}%)")
        
        # Create route files
        print("\n2. Creating Route Files")
        
        # All routes (only file needed for visualization)
        all_routes = create_routes_with_rural_urban(df_with_types, min_flows=1)
        
        # NEW: Create hierarchical data structure by year
        print("\n3. Creating Hierarchical Data Structure by Year")
        hierarchical_data = create_hierarchical_data_by_year(df_with_types)
    
    # OSRM路径获取 (可选)
    import sys
    use_osrm = '--osrm' in sys.argv
    top_100_only = '--top100' in sys.argv
    
    if use_osrm:
        print("\n3. Fetching OSRM Real Route Paths...")
        load_route_cache()
        
        # 加载之前的进度
        previous_progress = load_progress()
        start_from = 0
        if previous_progress:
            response = input(f"   Continue from previous progress ({previous_progress['processed']}/{previous_progress['total']})? (y/n): ")
            if response.lower() == 'y':
                start_from = previous_progress['processed']
        
        # 选择要处理的routes
        if top_100_only:
            routes_to_process = sorted(all_routes, key=lambda x: x['flows'], reverse=True)[:100]
            print(f"   Processing TOP 100 routes only")
        else:
            routes_to_process = all_routes
            print(f"   Processing ALL {len(all_routes)} routes")
        
        success_count = 0
        fail_count = 0
        batch_size = 50  # 每50个保存一次缓存和中间结果
        idx = start_from  # Initialize idx for error handling
        start_time = time.time()
        
        print(f"\n   Starting from route {start_from + 1}/{len(routes_to_process)}")
        print(f"   Cache will be saved every {batch_size} routes")
        print(f"   Press Ctrl+C to safely stop (progress will be saved)\n")
        
        try:
            for idx, route in enumerate(routes_to_process, 1):
                # 跳过已处理的
                if idx <= start_from:
                    # 检查是否已有path数据
                    if 'path' in route:
                        success_count += 1
                    continue
                
                # ✅ 关键修复：跳过已有路径的路线
                if 'path' in route and route['path']:
                    success_count += 1
                    print_progress_bar(idx, len(routes_to_process), success_count, fail_count, start_time, 'OSRM')
                    continue
                
                # 只处理有via_city的路线
                if 'via_city' not in route or not route['via_city'] or not route['via_city'].get('coordinates'):
                    # 显示进度条（跳过此路线）
                    print_progress_bar(idx, len(routes_to_process), success_count, fail_count, start_time, 'OSRM')
                    continue
                
                source_coords = route['source']['coordinates']
                via_coords = route['via_city']['coordinates']
                dest_coords = route['destination']['coordinates']
                route_id = f"{route['source']['name']}-{route['via_city']['name']}-{route['destination']['name']}"
                
                # 获取OSRM路径
                osrm_result = get_osrm_route(source_coords, via_coords, dest_coords, route_id)
                
                if osrm_result:
                    # 简化路径：从平均 2919 点减少到 ~50 点
                    original_points = len(osrm_result['path'])
                    simplified_path = simplify_path(osrm_result['path'], epsilon=0.001)
                    
                    route['path'] = simplified_path
                    route['distance_km'] = osrm_result['distance_km']
                    route['duration_hours'] = osrm_result['duration_hours']
                    route['path_points_original'] = original_points  # 记录原始点数
                    route['path_points_simplified'] = len(simplified_path)
                    success_count += 1
                else:
                    # 失败时保留直线（不添加path字段，前端会fallback到ArcLayer）
                    fail_count += 1
                
                # 更新进度条
                print_progress_bar(idx, len(routes_to_process), success_count, fail_count, start_time, 'OSRM')
                
                # 分批保存缓存、进度和中间结果
                if idx % batch_size == 0:
                    print()  # 新行
                    save_route_cache()
                    save_progress(idx, len(routes_to_process), route_id)
                    save_intermediate_results(hierarchical_data)
                    print(f"   💾 Checkpoint saved at {idx}/{len(routes_to_process)}")
                
                # 限流：每次请求间隔0.1秒
                time.sleep(0.1)
        
        except KeyboardInterrupt:
            print(f"\n\n⚠️  Interrupted by user!")
            elapsed = time.time() - start_time
            elapsed_str = str(timedelta(seconds=int(elapsed)))
            print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"   Processed: {idx}/{len(routes_to_process)} ({idx/len(routes_to_process)*100:.1f}%)")
            print(f"   Success:   {success_count} | Failed: {fail_count}")
            print(f"   Time:      {elapsed_str}")
            print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"\n   Saving progress before exit...")
            save_route_cache()
            save_progress(idx, len(routes_to_process), route_id if 'route_id' in locals() else 'interrupted')
            save_intermediate_results(hierarchical_data)
            print(f"   ✓ Progress saved. Run script again to continue from route {idx}")
            print(f"   ✓ Partial results saved to food_flows_by_year_temp.json")
            sys.exit(0)
        
        except Exception as e:
            print(f"\n\n❌ Error occurred: {e}")
            print(f"   Saving progress before exit...")
            save_route_cache()
            save_progress(idx, len(routes_to_process), route_id if 'route_id' in locals() else 'error')
            save_intermediate_results(hierarchical_data)
            print(f"   ✓ Progress saved to resume later")
            raise
        
        # 完成后显示最终进度条
        print_progress_bar(len(routes_to_process), len(routes_to_process), success_count, fail_count, start_time, 'OSRM')
        print()  # 新行
        
        # 最后保存一次
        save_route_cache()
        save_progress(len(routes_to_process), len(routes_to_process), 'completed')
        
        # 计算总耗时
        total_time = time.time() - start_time
        total_time_str = str(timedelta(seconds=int(total_time)))
        
        print(f"\n   ✅ OSRM Processing Complete!")
        print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"   ✓ Success:      {success_count:,} routes")
        print(f"   ✗ Failed:       {fail_count:,} routes")
        if success_count + fail_count > 0:
            print(f"   📊 Success Rate: {success_count/(success_count+fail_count)*100:.1f}%")
        print(f"   ⏱️  Total Time:   {total_time_str}")
        print(f"   🚀 Average Speed: {len(routes_to_process)/total_time:.2f} routes/s")
        print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # 完成后清理进度文件
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print(f"   ✓ Progress tracking completed, removed {PROGRESS_FILE}")
        
        # 重要：将 OSRM 路径数据同步到 hierarchical_data
        print(f"\n   Updating hierarchical data with OSRM paths...")
        paths_added = 0
        total_points_before = 0
        total_points_after = 0
        
        for route in all_routes:
            if 'path' in route and route['path']:
                # Find this route in hierarchical_data and add path
                # 使用 round(1) 与聚合精度保持一致
                src_x = round(route['source']['coordinates'][0], 1)
                src_y = round(route['source']['coordinates'][1], 1)
                dest_x = round(route['destination']['coordinates'][0], 1)
                dest_y = round(route['destination']['coordinates'][1], 1)
                
                # Determine city_name
                if route.get('via_city') and route['via_city'].get('name'):
                    city_name = route['via_city']['name']
                else:
                    city_name = 'direct'
                
                route_id = f"r_{src_x}_{src_y}_{dest_x}_{dest_y}_{city_name}"
                
                # Add path to all years this route appears in
                for year in route.get('years', []):
                    year_str = str(year)
                    if year_str in hierarchical_data and route_id in hierarchical_data[year_str]:
                        hierarchical_data[year_str][route_id]['path'] = route['path']
                        hierarchical_data[year_str][route_id]['distance_km'] = route.get('distance_km')
                        hierarchical_data[year_str][route_id]['duration_hours'] = route.get('duration_hours')
                        paths_added += 1
                        
                        # 统计简化效果
                        if 'path_points_original' in route:
                            total_points_before += route['path_points_original']
                            total_points_after += route['path_points_simplified']
        
        print(f"   ✓ Added {paths_added} OSRM paths to hierarchical data")
        if total_points_before > 0:
            reduction = (1 - total_points_after / total_points_before) * 100
            print(f"   ✓ Path simplification: {total_points_before:,} → {total_points_after:,} points ({reduction:.1f}% reduction)")
    
    # 保存JSON文件
    print("\n4. Saving Final Output File")
    
    try:
        # Only save hierarchical format by year (the one we actually use)
        # hierarchical_file already defined at the top of main()
        print(f"   Saving {hierarchical_file}...")
        with open(hierarchical_file, 'w', encoding='utf-8') as f:
            json.dump(hierarchical_data, f, indent=2, ensure_ascii=False)
        
        total_routes_hierarchical = sum(len(routes) for routes in hierarchical_data.values())
        print(f"   ✓ Saved {hierarchical_file} ({total_routes_hierarchical} routes across {len(hierarchical_data)} years)")
        
        # 清理临时文件
        # temp_file already defined at the top of main()
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"   ✓ Cleaned up temporary file")
        
    except Exception as e:
        print(f"\n❌ Error saving files: {e}")
        print(f"   Check intermediate results in *_temp.json files")
        raise
    
    print("\n" + "="*70)
    print("✅ RURAL-URBAN ANALYSIS COMPLETE!")
    print("="*70)
    
    if df_with_types is not None:
        print(f"\nKey Findings:")
        print(f"  Total flows analyzed: {len(df_with_types):,}")
        if overall_analysis.get('flow_patterns'):
            for flow_type, data in overall_analysis['flow_patterns'].items():
                print(f"  {data['label']:20s}: {data['count']:6,} flows ({data['percentage']:5.2f}%)")
    
    print(f"\nOutput File:")
    print(f"  - {hierarchical_file}")
    
    if use_osrm:
        # Count routes with paths
        routes_with_paths = sum(1 for year_routes in hierarchical_data.values() 
                               for route in year_routes.values() 
                               if 'path' in route and route['path'])
        total_routes = sum(len(routes) for routes in hierarchical_data.values())
        print(f"  - Total routes: {total_routes:,}")
        print(f"  - Routes with OSRM paths: {routes_with_paths:,} ({routes_with_paths/total_routes*100:.1f}%)")
    
    if use_osrm:
        print(f"\nCache Files:")
        print(f"  - {CACHE_FILE} (OSRM route cache, {len(ROUTE_CACHE):,} cached routes)")
        print(f"  - Can be safely deleted after completion")

if __name__ == '__main__':
    main()

