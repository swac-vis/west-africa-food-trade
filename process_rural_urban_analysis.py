#!/usr/bin/env python3
"""
Enhanced food flows processing with Rural-Urban analysis
Adds classification of flows by rural/urban patterns
"""

import pandas as pd
import json

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
    
    # === 按三点路线聚合: source_nam + city + destination_name + flow_type ===
    # 这样可以区分：同一source-destination但经过不同city的路线
    
    # Add city to grouping (using fillna to handle missing values)
    df['city_grouped'] = df['city'].fillna('direct')
    
    grouped = df.groupby([
        'source_nam',
        'destination_name', 
        'city_grouped',
        'flow_type'
    ])
    
    for key, route_df in grouped:
        source_name, dest_name, city_name, flow_type = key
        
        if len(route_df) < min_flows:
            continue
        
        # 从实际数据中获取坐标和国家信息（取第一条或众数）
        src_x = route_df['Source x'].median()
        src_y = route_df['Source y'].median()
        dest_x = route_df['Destination x'].median()
        dest_y = route_df['Destination y'].median()
        
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


def main():
    """Main processing function"""
    csv_path = 'Karg_food_flows_locations.csv'
    
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
    
    # Create route file with rural-urban info
    print("\n2. Creating Route File")
    
    # All routes (only file needed for visualization)
    all_routes = create_routes_with_rural_urban(df_with_types, min_flows=1)
    with open('food_flows_all_routes_rural_urban.json', 'w', encoding='utf-8') as f:
        json.dump(all_routes, f, indent=2, ensure_ascii=False)
    print(f"   ✓ Saved food_flows_all_routes_rural_urban.json ({len(all_routes)} routes)")
    
    print("\n" + "="*70)
    print("✅ RURAL-URBAN ANALYSIS COMPLETE!")
    print("="*70)
    print(f"\nKey Findings:")
    print(f"  Total flows analyzed: {len(df_with_types):,}")
    for flow_type, data in overall_analysis['flow_patterns'].items():
        print(f"  {data['label']:20s}: {data['count']:6,} flows ({data['percentage']:5.2f}%)")

if __name__ == '__main__':
    main()

