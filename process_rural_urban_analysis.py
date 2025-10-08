#!/usr/bin/env python3
"""
Enhanced food flows processing with Rural-Urban analysis
Adds classification of flows by rural/urban patterns
Now with OSRM real route paths
New hierarchical data structure by year
"""

import pandas as pd
import json
import requests
import time
import os
from pathlib import Path
from collections import defaultdict

# OSRM路径缓存
ROUTE_CACHE = {}
CACHE_FILE = 'osrm_route_cache.json'

def load_route_cache():
    """加载OSRM路径缓存"""
    global ROUTE_CACHE
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            ROUTE_CACHE = json.load(f)
        print(f"Loaded {len(ROUTE_CACHE)} cached routes")

def save_route_cache():
    """保存OSRM路径缓存"""
    with open(CACHE_FILE, 'w') as f:
        json.dump(ROUTE_CACHE, f)
    print(f"Saved {len(ROUTE_CACHE)} routes to cache")

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
    # 坐标舍入到小数点后3位（约111米精度），避免GPS微小偏差
    
    # Add city to grouping (using fillna to handle missing values)
    df['city_grouped'] = df['city'].fillna('direct')
    
    # Round coordinates for aggregation (3 decimal places ≈ 111m precision)
    df['src_x_rounded'] = df['Source x'].round(3)
    df['src_y_rounded'] = df['Source y'].round(3)
    df['dest_x_rounded'] = df['Destination x'].round(3)
    df['dest_y_rounded'] = df['Destination y'].round(3)
    
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
    
    # Round coordinates for aggregation
    df['src_x_rounded'] = df['Source x'].round(3)
    df['src_y_rounded'] = df['Source y'].round(3)
    df['dest_x_rounded'] = df['Destination x'].round(3)
    df['dest_y_rounded'] = df['Destination y'].round(3)
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
    # Use the fixed CSV with corrected encoding
    csv_path = 'Karg_food_flows_locations_fixed.csv'
    
    # Load and analyze
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
        
        # 选择要处理的routes
        if top_100_only:
            routes_to_process = sorted(all_routes, key=lambda x: x['flows'], reverse=True)[:100]
            print(f"   Processing TOP 100 routes only")
        else:
            routes_to_process = all_routes
            print(f"   Processing ALL {len(all_routes)} routes")
        
        success_count = 0
        fail_count = 0
        batch_size = 50  # 每50个保存一次缓存
        
        for idx, route in enumerate(routes_to_process, 1):
            source_coords = route['source']['coordinates']
            via_coords = route['via_city']['coordinates']
            dest_coords = route['destination']['coordinates']
            route_id = f"{route['source']['name']}-{route['via_city']['name']}-{route['destination']['name']}"
            
            # 获取OSRM路径
            osrm_result = get_osrm_route(source_coords, via_coords, dest_coords, route_id)
            
            if osrm_result:
                route['path'] = osrm_result['path']
                route['distance_km'] = osrm_result['distance_km']
                route['duration_hours'] = osrm_result['duration_hours']
                success_count += 1
                print(f"   [{idx}/{len(routes_to_process)}] ✓ {route_id[:50]:50s} ({len(osrm_result['path'])} points, {osrm_result['distance_km']:.1f}km)")
            else:
                # 失败时保留直线（不添加path字段，前端会fallback到ArcLayer）
                fail_count += 1
                print(f"   [{idx}/{len(routes_to_process)}] ✗ {route_id[:50]:50s} (using straight line)")
            
            # 分批保存缓存
            if idx % batch_size == 0:
                save_route_cache()
                print(f"   💾 Saved cache at {idx}/{len(routes_to_process)}")
            
            # 限流：每次请求间隔0.1秒
            time.sleep(0.1)
        
        # 最后保存一次缓存
        save_route_cache()
        
        print(f"\n   OSRM Summary:")
        print(f"   ✓ Success: {success_count}")
        print(f"   ✗ Failed:  {fail_count}")
        print(f"   📊 Success Rate: {success_count/(success_count+fail_count)*100:.1f}%")
    
    # 保存JSON文件
    print("\n4. Saving Output Files")
    
    # Save original format (for backward compatibility)
    output_file = 'food_flows_all_routes_rural_urban_with_paths.json' if use_osrm else 'food_flows_all_routes_rural_urban.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_routes, f, indent=2, ensure_ascii=False)
    print(f"   ✓ Saved {output_file} ({len(all_routes)} routes)")
    
    # Save new hierarchical format by year
    hierarchical_file = 'food_flows_by_year.json'
    with open(hierarchical_file, 'w', encoding='utf-8') as f:
        json.dump(hierarchical_data, f, indent=2, ensure_ascii=False)
    
    total_routes_hierarchical = sum(len(routes) for routes in hierarchical_data.values())
    print(f"   ✓ Saved {hierarchical_file} ({total_routes_hierarchical} routes across {len(hierarchical_data)} years)")
    
    print("\n" + "="*70)
    print("✅ RURAL-URBAN ANALYSIS COMPLETE!")
    print("="*70)
    print(f"\nKey Findings:")
    print(f"  Total flows analyzed: {len(df_with_types):,}")
    for flow_type, data in overall_analysis['flow_patterns'].items():
        print(f"  {data['label']:20s}: {data['count']:6,} flows ({data['percentage']:5.2f}%)")
    print(f"\nOutput Files:")
    print(f"  - {output_file}")
    print(f"  - {hierarchical_file}")

if __name__ == '__main__':
    main()

