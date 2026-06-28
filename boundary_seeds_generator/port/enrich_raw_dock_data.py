import csv
import json
import pygeohash as pgh

# Configuration
INPUT_CSV = "raw_ports_geometry.csv"
OUTPUT_CSV = "processed_ports_lookup.csv"

# Precision 5 creates a grid cell of approximately 4.9km x 4.9km.
# Precision 4 creates a grid cell of approximately 39km x 19km.
GEOHASH_PRECISION = 5 

def process_ports():
    processed_records = []
    
    print(f"Reading raw data from {INPUT_CSV}...")
    
    with open(INPUT_CSV, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            try:
                coords = json.loads(row['geometry_json'])
                
                if not coords:
                    continue
                
                lons = [pt[0] for pt in coords]
                lats = [pt[1] for pt in coords]
                
                min_lon, max_lon = min(lons), max(lons)
                min_lat, max_lat = min(lats), max(lats)
                
                center_lon = (min_lon + max_lon) / 2.0
                center_lat = (min_lat + max_lat) / 2.0
                
                grid_id = pgh.encode(center_lat, center_lon, precision=GEOHASH_PRECISION)
                clean_name = row['name'].encode('ascii', errors='ignore').decode('ascii')

                # --- NEW GEOMETRY FORMATTING BLOCK ---
                # 1. GeoJSON Polygons require the first and last point to be identical
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                    
                # 2. Build ONLY the raw Polygon geometry object
                geometry_only = {
                    "type": "Polygon",
                    "coordinates": [coords] # Polygons require a nested array
                }
                # ------------------------------------

                processed_records.append({
                    'port_id': row['port_id'],
                    'name': clean_name,
                    'grid_id': grid_id,
                    'min_lat': min_lat,
                    'max_lat': max_lat,
                    'min_lon': min_lon,
                    'max_lon': max_lon,
                    # Dump the bare Polygon dictionary instead of the Feature wrapper
                    'geometry_json': json.dumps(geometry_only) 
                })
                
            except json.JSONDecodeError:
                print(f"Skipping row {row['port_id']} due to invalid JSON.")
            except Exception as e:
                print(f"Error processing row {row['port_id']}: {e}")

    print(f"Writing {len(processed_records)} enriched records to {OUTPUT_CSV}...")
    
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = [
            'port_id', 'name', 'grid_id', 
            'min_lat', 'max_lat', 'min_lon', 'max_lon', 
            'geometry_json'
        ]
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in processed_records:
            writer.writerow(record)
            
    print("Processing complete. Data is ready for dbt seed.")

if __name__ == "__main__":
    process_ports()