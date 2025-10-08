# Data Difference Report

## Dataset Comparison

### Files Compared
1. **Food_flow_data_v1.csv** - Original raw data
2. **Karg_food_flows_locations_fixed.csv** - Geocoded and processed data (encoding errors fixed)

---

## Summary Statistics

| Metric | Food_flow_data_v1.csv | Karg_food_flows_locations_fixed.csv |
|--------|----------------------|-------------------------------------|
| **Total Records** | 113,946 | 80,762 |
| **Difference** | - | **-33,184 (-29.1%)** |
| **Columns** | 34 (original fields) | 76 (with geocoding) |
| **Unique Source Locations** | 1,473 | 1,459 |
| **Unique Original IDs** | 58,141 | 45,759 |

---

## Why 33,184 Records Are Missing in Karg

### Root Cause: **Geocoding Failure**

The Karg dataset is derived from Food_flow by adding geographic coordinates. Records without valid coordinates were excluded.

### Breakdown of Missing Records

```
Food_flow_data_v1.csv:        113,946 records
├─ With complete coordinates:  80,762 records  ← Became Karg dataset
└─ Missing coordinates:        33,184 records  ← Excluded
   ├─ Missing both:            29,652 (89.3%)
   ├─ Missing source only:      1,880 (5.7%)
   └─ Missing destination only: 1,652 (5.0%)
```

### Temporal Distribution of Missing Data
- **2016-2017**: 22,594 records (68.1%) ⚠️ Most recent data
- **2014-2015**: 9,423 records (28.4%)
- **2013**: 1,167 records (3.5%)

### Top Data Sources with Missing Coordinates
1. Moribabougou: 2,779 records
2. Kassela: 2,609 records
3. Sebeninkoro: 2,606 records (note: 4,504 successfully geocoded)
4. Pô road: 2,330 records
5. Sénou: 2,236 records (note: 4,445 successfully geocoded)

---

## Why Geocoding Failed

### Likely Reasons:
1. **Encoding errors** in location names (e.g., `√©` instead of `é`)
   - Already fixed in `Karg_food_flows_locations_fixed.csv`
2. **Vague location names** (e.g., "Tamale (not specified)")
3. **Spelling variations** or typos
4. **New/unofficial locations** not in geocoding databases
5. **Geocoding service failures** during processing

---

## Key Differences Between Datasets

### Food_flow_data_v1.csv (Original)
✅ **More complete**: 41% more records  
✅ **Raw data**: Original collector's records  
✅ **Geometry fields**: Has `source_geometry` and `destination_geometry` (WKT POINT format)  
✅ **Commodity categories**: Includes `commodity_category` (Cereal, Vegetable, etc.)  
❌ Missing structured geographic metadata  

### Karg_food_flows_locations_fixed.csv (Geocoded)
✅ **100% geocoded**: All records have valid coordinates  
✅ **Rich geographic metadata**: Country codes, admin boundaries, distances  
✅ **Cross-border analysis**: Fields like "Crosses international border?"  
✅ **Clean encoding**: Fixed `√©` → `é` and other UTF-8 errors  
✅ **Coordinate precision**: Separate X/Y columns for source and destination  
❌ Missing 29.1% of original data due to geocoding failures  

---

## Column Structure Comparison

### Karg-Specific Columns (Geographic enrichment)
- `Source x`, `Source y`, `Destination x`, `Destination y`
- `Source_country_code`, `Source_country_name`, `Source_admin1`, `Source_admin2`
- `Dest_country_code`, `Dest_country_name`, `Dest_admin1`, `Dest_admin2`
- `distance_to_source_km`, `distance_1`
- `Crosses international border?`, `International border`

### Food_flow-Specific Columns
- `source_geometry`, `destination_geometry` (POINT WKT format)
- `commodity_name_gen`, `commodity_category`

---

## Recommendation for Analysis

### Current Status
- **Using**: `Karg_food_flows_locations_fixed.csv` (80,762 records)
- **Reason**: 100% geocoded, ready for OSRM path calculation

### Options for Recovery
1. **Keep current dataset** (70.9% coverage, high quality)
2. **Re-geocode missing 33k records** using fixed encoding
3. **Hybrid approach**: Use Karg + manually fix top missing locations

---

## Data Quality Notes

### Encoding Fixes Applied
- Fixed 213 location names with encoding errors
- `√©` → `é` (e.g., "March√©" → "Marché")
- `√¥` → `ô` (e.g., "C√¥te d'Ivoire" → "Côte d'Ivoire")
- Removed invalid symbols: `¢`, `®`, `≠`, etc.

### Final Statistics
- **Total unique locations**: 1,796
- **Locations with encoding errors**: 214 (11.9%)
- **Successfully fixed**: 213 (99.5%)

---

**Date**: October 8, 2025  
**Analyst**: Data processing for West Africa Food Trade Visualization

