#!/usr/bin/env python3
"""
查找并补充缺失的 OSRM 路径
"""

import json
import requests
import time

def simplify_path(points, epsilon=0.001):
    """Douglas-Peucker 算法简化路径"""
    if len(points) < 3:
        return points
    
    def perpendicular_distance(point, line_start, line_end):
        x0, y0 = point
        x1, y1 = line_start
        x2, y2 = line_end
        dx = x2 - x1
        dy = y2 - y1
        if dx == 0 and dy == 0:
            return ((x0 - x1)**2 + (y0 - y1)**2)**0.5
        t = max(0, min(1, ((x0 - x1) * dx + (y0 - y1) * dy) / (dx * dx + dy * dy)))
        proj_x = x1 + t * dx
        proj_y = y1 + t * dy
        return ((x0 - proj_x)**2 + (y0 - proj_y)**2)**0.5
    
    def rdp(points, epsilon):
        if len(points) < 3:
            return points
        max_dist = 0
        max_index = 0
        for i in range(1, len(points) - 1):
            dist = perpendicular_distance(points[i], points[0], points[-1])
            if dist > max_dist:
                max_dist = dist
                max_index = i
        if max_dist > epsilon:
            left = rdp(points[:max_index + 1], epsilon)
            right = rdp(points[max_index:], epsilon)
            return left[:-1] + right
        else:
            return [points[0], points[-1]]
    
    return rdp(points, epsilon)

def get_osrm_route(source_coords, via_coords, dest_coords, max_retries=3):
    """获取 OSRM 路径（带重试）"""
    for attempt in range(max_retries):
        try:
            coords_str = f"{source_coords[0]},{source_coords[1]};{via_coords[0]},{via_coords[1]};{dest_coords[0]},{dest_coords[1]}"
            url = f"http://router.project-osrm.org/route/v1/driving/{coords_str}"
            params = {
                'overview': 'full',
                'geometries': 'geojson'
            }
            
            response = requests.get(url, params=params, timeout=20)
            
            if response.ok:
                data = response.json()
                if data.get('routes'):
                    route = data['routes'][0]
                    return {
                        'path': route['geometry']['coordinates'],
                        'distance_km': route['distance'] / 1000,
                        'duration_hours': route['duration'] / 3600
                    }
            
            if response.status_code >= 400:
                return None
                
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"   Timeout, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                return None
                
        except Exception as e:
            print(f"   Error: {e}")
            return None
    
    return None

def main():
    filename = 'food_flows_by_year_round1.json'
    
    print(f"Loading {filename}...")
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 查找缺失路径的路线
    print("\nSearching for routes without paths...")
    missing_routes = []
    
    for year, year_routes in data.items():
        for route_id, route in year_routes.items():
            if 'via_city' in route and route['via_city'] and route['via_city'].get('coordinates'):
                if 'path' not in route or not route['path']:
                    missing_routes.append({
                        'year': year,
                        'route_id': route_id,
                        'route': route
                    })
    
    print(f"Found {len(missing_routes)} routes without paths")
    
    if len(missing_routes) == 0:
        print("✅ All routes already have paths!")
        return
    
    # 处理缺失的路线
    print(f"\nProcessing {len(missing_routes)} missing routes...\n")
    
    for i, item in enumerate(missing_routes, 1):
        year = item['year']
        route_id = item['route_id']
        route = item['route']
        
        source_coords = route['source']['coordinates']
        via_coords = route['via_city']['coordinates']
        dest_coords = route['destination']['coordinates']
        
        print(f"{i}/{len(missing_routes)}: {route['source']['name']} → {route['via_city']['name']} → {route['destination']['name']}")
        
        # 获取路径
        osrm_result = get_osrm_route(source_coords, via_coords, dest_coords)
        
        if osrm_result:
            # 简化路径
            original_points = len(osrm_result['path'])
            simplified_path = simplify_path(osrm_result['path'], epsilon=0.001)
            
            # 添加到数据
            data[year][route_id]['path'] = simplified_path
            data[year][route_id]['distance_km'] = osrm_result['distance_km']
            data[year][route_id]['duration_hours'] = osrm_result['duration_hours']
            
            print(f"   ✓ Success! Path simplified: {original_points} → {len(simplified_path)} points")
        else:
            print(f"   ✗ Failed - will use straight line")
        
        time.sleep(0.2)  # 限流
    
    # 保存更新后的文件
    print(f"\nSaving updated {filename}...")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print("✅ Done!")

if __name__ == '__main__':
    main()

