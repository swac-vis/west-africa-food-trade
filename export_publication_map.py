#!/usr/bin/env python3
"""
Export food flows map as high-quality static image for publication

Generates publication-ready maps with:
- White/light background
- Real road paths
- Clean vector graphics (PDF/SVG) or high-DPI raster (PNG)
- Customizable filters (year, commodity, flow type, etc.)

USAGE:
    Basic:              python export_publication_map.py
    Filter by year:     python export_publication_map.py --year 2015
    Filter by commodity: python export_publication_map.py --commodity Maize
    High DPI PNG:       python export_publication_map.py --format png --dpi 300
    Vector PDF:         python export_publication_map.py --format pdf
    Top routes only:    python export_publication_map.py --top 100
    
    Single city:
    python export_publication_map.py --via-city Ouagadougou --format pdf --title "Food Flows via Ouagadougou"
    
    Color modes (matching web visualization):
      International:    python export_publication_map.py --color-mode international
      Rural-Urban:      python export_publication_map.py --color-mode rural_urban
      Source-Dest:      python export_publication_map.py --color-mode source_destination
    
    All routes:
    python export_publication_map.py --format pdf --title "West African Food Flows (All Routes, 2013-2017)"
OUTPUT:
    Default: food_flows_map_publication.pdf
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import LineCollection
import numpy as np
import argparse
from datetime import datetime

# Try to import cartopy for map projections
try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False
    print("⚠️  cartopy not available. Install with: conda install -c conda-forge cartopy")
    print("   Will use basic lat/lon plotting instead.\n")

# Try to import contextily for basemaps (optional)
try:
    import contextily as cx
    HAS_CONTEXTILY = True
except ImportError:
    HAS_CONTEXTILY = False
    print("⚠️  contextily not available. Install with: pip install contextily")
    print("   Maps will be generated without basemap tiles.\n")

def load_data(json_file):
    """Load hierarchical food flows data"""
    print(f"Loading data from {json_file}...")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Flatten hierarchical data
    routes = []
    for year_str, year_routes in data.items():
        for route_id, route_data in year_routes.items():
            route = route_data.copy()
            route['year'] = int(year_str)
            route['route_id'] = route_id
            routes.append(route)
    
    print(f"✓ Loaded {len(routes)} routes across {len(data)} years")
    return routes

def filter_routes(routes, year=None, commodity=None, flow_type=None, 
                  international=None, via_city=None, top=None):
    """Filter routes based on criteria"""
    filtered = routes.copy()
    
    if year is not None:
        filtered = [r for r in filtered if r['year'] == year]
        print(f"  Filter by year {year}: {len(filtered)} routes")
    
    if commodity is not None:
        filtered = [r for r in filtered if commodity in r.get('commodity', {})]
        print(f"  Filter by commodity '{commodity}': {len(filtered)} routes")
    
    if flow_type is not None:
        filtered = [r for r in filtered if r.get('flow_type') == flow_type]
        print(f"  Filter by flow type '{flow_type}': {len(filtered)} routes")
    
    if international is not None:
        filtered = [r for r in filtered if r.get('is_international') == international]
        label = 'international' if international else 'domestic'
        print(f"  Filter by {label} trade: {len(filtered)} routes")
    
    if via_city is not None:
        filtered = [r for r in filtered 
                   if r.get('via_city', {}).get('name') == via_city]
        print(f"  Filter by via city '{via_city}': {len(filtered)} routes")
    
    if top is not None:
        filtered = sorted(filtered, key=lambda r: r.get('quantity', 0), reverse=True)[:top]
        print(f"  Top {top} routes by quantity: {len(filtered)} routes")
    
    return filtered

def plot_routes(routes, output_file='food_flows_map_publication.pdf', 
                dpi=300, figsize=(16, 12), use_basemap=True,
                background_color='white', title=None, color_mode='international'):
    """
    Plot routes as publication-quality map with proper projection
    
    Args:
        routes: List of route objects
        output_file: Output filename (.pdf, .png, or .svg)
        dpi: DPI for raster outputs
        figsize: Figure size in inches
        use_basemap: Whether to add basemap tiles
        background_color: Background color
        title: Optional title
        color_mode: Color encoding mode ('international', 'rural_urban', or 'source_destination')
    """
    print(f"\nGenerating map...")
    print(f"  Routes to plot: {len(routes)}")
    print(f"  Output: {output_file}")
    print(f"  DPI: {dpi}")
    print(f"  Size: {figsize[0]}x{figsize[1]} inches")
    
    # Create figure with cartopy projection for proper basemap
    if HAS_CARTOPY:
        projection = ccrs.PlateCarree()
        fig, ax = plt.subplots(figsize=figsize, facecolor=background_color,
                              subplot_kw={'projection': projection})
        ax.set_facecolor(background_color)
    else:
        fig, ax = plt.subplots(figsize=figsize, facecolor=background_color)
        ax.set_facecolor(background_color)
    
    # Collect all coordinates to determine bounds
    all_lons = []
    all_lats = []
    
    for route in routes:
        src_coords = route['source']['coordinates']
        dest_coords = route['destination']['coordinates']
        all_lons.extend([src_coords[0], dest_coords[0]])
        all_lats.extend([src_coords[1], dest_coords[1]])
    
    # Set map extent with some padding
    lon_min, lon_max = min(all_lons), max(all_lons)
    lat_min, lat_max = min(all_lats), max(all_lats)
    lon_padding = (lon_max - lon_min) * 0.1
    lat_padding = (lat_max - lat_min) * 0.1
    
    if HAS_CARTOPY:
        ax.set_extent([lon_min - lon_padding, lon_max + lon_padding,
                      lat_min - lat_padding, lat_max + lat_padding], 
                      crs=ccrs.PlateCarree())
    else:
        ax.set_xlim(lon_min - lon_padding, lon_max + lon_padding)
        ax.set_ylim(lat_min - lat_padding, lat_max + lat_padding)
    
    # Add complete light basemap
    if use_basemap and HAS_CARTOPY:
        print("  Adding basemap features...")
        ax.add_feature(cfeature.LAND, facecolor='#f9f9f9', zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor='#e8f4f8', zorder=0)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5, edgecolor='#aaa', zorder=1)
        ax.add_feature(cfeature.BORDERS, linewidth=0.4, edgecolor='#ccc', 
                      linestyle='-', zorder=1, alpha=0.6)
    
    # Color schemes - matching index.html
    # Mode 1: International vs Domestic
    color_international = '#e53e3e'  # Red
    color_domestic = '#3182ce'  # Blue
    
    # Mode 2: Rural vs Urban
    color_rural = '#2A50EA'  # Blue for rural
    color_urban = '#FBE819'  # Yellow for urban
    
    # Mode 3: Source vs Destination
    color_source = '#10b981'  # Green for source
    color_destination = '#ef4444'  # Red for destination
    
    print(f"  Drawing {len(routes)} routes...")
    print(f"  Color mode: {color_mode}")
    
    # Set transform if using cartopy
    transform = ccrs.PlateCarree() if HAS_CARTOPY else None
    
    for route in routes:
        src_coords = route['source']['coordinates']
        dest_coords = route['destination']['coordinates']
        
        # Width by quantity (log scale) - thinner lines
        quantity = route.get('quantity', 0)
        width = np.log10(quantity + 1) * 0.4 if quantity > 0 else 0.4
        
        # Use real path if available, otherwise straight line
        if 'path' in route and route['path']:
            path_coords = np.array(route['path'])
            lons = path_coords[:, 0]
            lats = path_coords[:, 1]
            
            # Create gradient colors for the path
            num_points = len(path_coords)
            
            if color_mode == 'international':
                # Uniform color (no gradient for international mode)
                is_intl = route.get('is_international', False)
                color = color_international if is_intl else color_domestic
                
                # Plot with single color
                if HAS_CARTOPY:
                    ax.plot(lons, lats, color=color, linewidth=width, 
                           alpha=0.3, zorder=2, solid_capstyle='round',
                           transform=transform)
                else:
                    ax.plot(lons, lats, color=color, linewidth=width, 
                           alpha=0.3, zorder=2, solid_capstyle='round')
                           
            elif color_mode == 'rural_urban':
                # Color by flow type (four distinct colors)
                flow_type = route.get('flow_type', '')
                if flow_type == 'rural_to_urban':
                    color = '#2ecc71'  # Green
                elif flow_type == 'urban_to_rural':
                    color = '#3498db'  # Blue
                elif flow_type == 'rural_to_rural':
                    color = '#f1c40f'  # Yellow
                elif flow_type == 'urban_to_urban':
                    color = '#9b59b6'  # Purple
                else:
                    color = '#95a5a6'  # Gray fallback
                
                # Plot with uniform color
                if HAS_CARTOPY:
                    ax.plot(lons, lats, color=color, linewidth=width, 
                           alpha=0.3, zorder=2, solid_capstyle='round',
                           transform=transform)
                else:
                    ax.plot(lons, lats, color=color, linewidth=width, 
                           alpha=0.3, zorder=2, solid_capstyle='round')
                               
            else:  # source_destination
                # Gradient from green (source) to red (destination)
                def hex_to_rgb(hex_color):
                    hex_color = hex_color.lstrip('#')
                    return tuple(int(hex_color[i:i+2], 16)/255 for i in (0, 2, 4))
                
                src_color = hex_to_rgb(color_source)
                dest_color = hex_to_rgb(color_destination)
                
                # Create gradient segments
                for i in range(num_points - 1):
                    t = i / (num_points - 1)
                    r = src_color[0] * (1 - t) + dest_color[0] * t
                    g = src_color[1] * (1 - t) + dest_color[1] * t
                    b = src_color[2] * (1 - t) + dest_color[2] * t
                    
                    segment_color = (r, g, b)
                    
                    if HAS_CARTOPY:
                        ax.plot([lons[i], lons[i+1]], [lats[i], lats[i+1]], 
                               color=segment_color, linewidth=width, 
                               alpha=0.3, zorder=2, solid_capstyle='round',
                               transform=transform)
                    else:
                        ax.plot([lons[i], lons[i+1]], [lats[i], lats[i+1]], 
                               color=segment_color, linewidth=width, 
                               alpha=0.3, zorder=2, solid_capstyle='round')
        else:
            # Straight line fallback - use single color
            if color_mode == 'international':
                is_intl = route.get('is_international', False)
                color = color_international if is_intl else color_domestic
            elif color_mode == 'rural_urban':
                # Use source color for straight lines
                src_is_urban = route['source'].get('is_urban', False)
                color = color_urban if src_is_urban else color_rural
            else:  # source_destination
                color = color_source
            
            if HAS_CARTOPY:
                ax.plot([src_coords[0], dest_coords[0]], 
                       [src_coords[1], dest_coords[1]], 
                       color=color, linewidth=width, 
                       alpha=0.3, zorder=2, solid_capstyle='round',
                       transform=transform)
            else:
                ax.plot([src_coords[0], dest_coords[0]], 
                       [src_coords[1], dest_coords[1]], 
                       color=color, linewidth=width, 
                       alpha=0.3, zorder=2, solid_capstyle='round')
    
    # Mark transit hub cities
    hub_cities = {}
    for route in routes:
        via_city = route.get('via_city', {})
        if via_city and via_city.get('name') and via_city.get('coordinates'):
            city_name = via_city['name']
            if city_name not in hub_cities:
                hub_cities[city_name] = via_city['coordinates']
    
    # Plot and label hub cities
    if hub_cities:
        print(f"  Marking {len(hub_cities)} hub cities...")
        for city_name, coords in hub_cities.items():
            lon, lat = coords
            
            # Plot city marker
            if HAS_CARTOPY:
                ax.scatter([lon], [lat], c='#ff6b6b', s=120, marker='*', 
                          edgecolors='black', linewidths=1.5, zorder=10,
                          transform=transform)
                # Add city label
                ax.text(lon, lat + 0.3, city_name, fontsize=11, 
                       fontweight='bold', ha='center', va='bottom',
                       color='#333', zorder=11, transform=transform,
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                edgecolor='none', alpha=0.8))
            else:
                ax.scatter([lon], [lat], c='#ff6b6b', s=120, marker='*', 
                          edgecolors='black', linewidths=1.5, zorder=10)
                ax.text(lon, lat + 0.3, city_name, fontsize=11, 
                       fontweight='bold', ha='center', va='bottom',
                       color='#333', zorder=11,
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                edgecolor='none', alpha=0.8))
    
    # Add legend based on color mode
    legend_elements = []
    
    if color_mode == 'international':
        legend_elements = [
            mpatches.Patch(color=color_international, label='International', alpha=0.3),
            mpatches.Patch(color=color_domestic, label='Domestic', alpha=0.3)
        ]
    elif color_mode == 'rural_urban':
        legend_elements = [
            mpatches.Patch(color='#2ecc71', label='Rural → Urban', alpha=0.3),
            mpatches.Patch(color='#3498db', label='Urban → Rural', alpha=0.3),
            mpatches.Patch(color='#f1c40f', label='Rural → Rural', alpha=0.3),
            mpatches.Patch(color='#9b59b6', label='Urban → Urban', alpha=0.3)
        ]
    else:  # source_destination
        legend_elements = [
            mpatches.Patch(color=color_source, label='Source', alpha=0.3),
            mpatches.Patch(color=color_destination, label='Destination', alpha=0.3)
        ]
    
    ax.legend(handles=legend_elements, loc='upper left', 
             framealpha=0.95, fontsize=9, edgecolor='#ddd', fancybox=False)
    
    # Title
    if title:
        ax.set_title(title, fontsize=16, fontweight='normal', pad=20, color='#333')
    else:
        ax.set_title('African Food Flows', fontsize=16, fontweight='normal', pad=20, color='#333')
    
    # Clean, minimal axes
    if HAS_CARTOPY:
        gl = ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.15, 
                         linestyle=':', color='#bbb')
        gl.top_labels = False
        gl.right_labels = False
        gl.xlabel_style = {'size': 9, 'color': '#666'}
        gl.ylabel_style = {'size': 9, 'color': '#666'}
    else:
        ax.set_xlabel('Longitude', fontsize=10, color='#666')
        ax.set_ylabel('Latitude', fontsize=10, color='#666')
        ax.tick_params(labelsize=9, colors='#666')
        ax.grid(True, alpha=0.15, linewidth=0.5, color='#ccc', linestyle=':')
    
    # Tight layout
    plt.tight_layout()
    
    # Save
    print(f"  Saving to {output_file}...")
    if output_file.endswith('.pdf'):
        plt.savefig(output_file, format='pdf', dpi=dpi, 
                   bbox_inches='tight', facecolor=background_color)
    elif output_file.endswith('.svg'):
        plt.savefig(output_file, format='svg', 
                   bbox_inches='tight', facecolor=background_color)
    else:  # PNG or other
        plt.savefig(output_file, format='png', dpi=dpi, 
                   bbox_inches='tight', facecolor=background_color)
    
    print(f"✅ Map saved to {output_file}")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Export publication-quality food flows map')
    parser.add_argument('--data', default='food_flows_by_year_round1.json',
                       help='Input JSON data file')
    parser.add_argument('--year', type=int, help='Filter by year (e.g., 2015)')
    parser.add_argument('--commodity', help='Filter by commodity (e.g., Maize)')
    parser.add_argument('--flow-type', choices=['rural_to_urban', 'urban_to_rural', 
                                                  'rural_to_rural', 'urban_to_urban'],
                       help='Filter by flow type')
    parser.add_argument('--international', action='store_true', 
                       help='Show only international flows')
    parser.add_argument('--domestic', action='store_true',
                       help='Show only domestic flows')
    parser.add_argument('--via-city', help='Filter by transit city (e.g., Bamenda, Bamako)')
    parser.add_argument('--top', type=int, help='Show only top N routes by quantity')
    parser.add_argument('--color-mode', choices=['international', 'rural_urban', 'source_destination'],
                       default='international',
                       help='Color encoding mode (default: international)')
    parser.add_argument('--format', choices=['pdf', 'png', 'svg'], default='pdf',
                       help='Output format (default: pdf)')
    parser.add_argument('--dpi', type=int, default=300,
                       help='DPI for raster outputs (default: 300)')
    parser.add_argument('--width', type=float, default=16,
                       help='Figure width in inches (default: 16)')
    parser.add_argument('--height', type=float, default=12,
                       help='Figure height in inches (default: 12)')
    parser.add_argument('--no-basemap', action='store_true',
                       help='Do not add basemap tiles')
    parser.add_argument('--background', default='white',
                       help='Background color (default: white)')
    parser.add_argument('--output', help='Output filename (auto-generated if not specified)')
    parser.add_argument('--title', help='Map title')
    
    args = parser.parse_args()
    
    print("="*70)
    print("FOOD FLOWS MAP - PUBLICATION EXPORT")
    print("="*70 + "\n")
    
    # Load data
    routes = load_data(args.data)
    
    # Apply filters
    print("\nApplying filters...")
    international = None
    if args.international:
        international = True
    elif args.domestic:
        international = False
    
    filtered_routes = filter_routes(
        routes,
        year=args.year,
        commodity=args.commodity,
        flow_type=args.flow_type,
        international=international,
        via_city=args.via_city,
        top=args.top
    )
    
    if len(filtered_routes) == 0:
        print("\n❌ No routes match the specified filters!")
        return
    
    # Generate output filename if not specified
    if args.output:
        output_file = args.output
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filters = []
        if args.year:
            filters.append(f"year{args.year}")
        if args.commodity:
            filters.append(args.commodity.lower().replace(' ', '_'))
        if args.top:
            filters.append(f"top{args.top}")
        
        filter_str = '_' + '_'.join(filters) if filters else ''
        output_file = f"food_flows_map{filter_str}_{timestamp}.{args.format}"
    
    # Plot
    plot_routes(
        filtered_routes,
        output_file=output_file,
        dpi=args.dpi,
        figsize=(args.width, args.height),
        use_basemap=not args.no_basemap and HAS_CARTOPY,
        background_color=args.background,
        title=args.title,
        color_mode=args.color_mode
    )
    
    print("\n" + "="*70)
    print("✅ EXPORT COMPLETE")
    print("="*70)
    print(f"\nOutput file: {output_file}")
    print(f"Routes plotted: {len(filtered_routes):,}")
    
    # Show statistics
    total_quantity = sum(r.get('quantity', 0) for r in filtered_routes)
    intl_count = sum(1 for r in filtered_routes if r.get('is_international'))
    
    print(f"\nStatistics:")
    print(f"  Total quantity: {total_quantity:,.0f} kg")
    print(f"  International: {intl_count} ({intl_count/len(filtered_routes)*100:.1f}%)")
    print(f"  Domestic: {len(filtered_routes)-intl_count} ({(len(filtered_routes)-intl_count)/len(filtered_routes)*100:.1f}%)")
    
    # Routes with paths
    with_paths = sum(1 for r in filtered_routes if 'path' in r and r['path'])
    print(f"  With road paths: {with_paths} ({with_paths/len(filtered_routes)*100:.1f}%)")

if __name__ == '__main__':
    main()

