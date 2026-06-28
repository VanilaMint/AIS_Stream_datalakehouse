import requests
import csv
import time
import json

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUTPUT_CSV = "raw_ports_geometry.csv"

# Divide the world into a 4x4 grid (Lat, Lon) to prevent API timeouts
grid_steps = [
    (-90, -180, 0, -90),     (0, -180, 90, -90),
    (-90, -90, 0, 0),        (0, -90, 90, 0),
    (-90, 0, 0, 90),         (0, 0, 90, 90),
    (-90, 90, 0, 180),       (0, 90, 90, 180)
]

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 15

def fetch_geometry(min_lat, min_lon, max_lat, max_lon):
    """Fetches full geometry for ways and relations within a bounding box."""
    
    query = f"""
    [out:json][timeout:180];
    (
      way["landuse"="port"]({min_lat},{min_lon},{max_lat},{max_lon});
      relation["landuse"="port"]({min_lat},{min_lon},{max_lat},{max_lon});
      
      way["industrial"="port"]({min_lat},{min_lon},{max_lat},{max_lon});
      relation["industrial"="port"]({min_lat},{min_lon},{max_lat},{max_lon});
      
      way["waterway"="dock"]({min_lat},{min_lon},{max_lat},{max_lon});
      relation["waterway"="dock"]({min_lat},{min_lon},{max_lat},{max_lon});
      
    );
    out geom;
    """
    
    headers = {
        'User-Agent': 'Portdata/1.0 (minhhh36@gmail.com)'
    }
    
    response = requests.post(
        OVERPASS_URL, 
        data={'data': query}, 
        headers=headers
    )
    
    response.raise_for_status()
    return response.json()

def main():
    extracted_docks = []
    seen_way_ids = set() # Global tracker to ensure zero duplicate records
    
    print(f"Starting flat global geometry extraction across {len(grid_steps)} zones...")
    
    for i, (min_lat, min_lon, max_lat, max_lon) in enumerate(grid_steps):
        print(f"\nFetching Zone {i+1}/{len(grid_steps)}...")
        
        attempt = 0
        success = False
        
        while attempt < MAX_RETRIES and not success:
            try:
                data = fetch_geometry(min_lat, min_lon, max_lat, max_lon)
                elements = data.get('elements', [])
                
                ways = []
                relations = []
                
                for el in elements:
                    if el['type'] == 'relation': 
                        relations.append(el)
                    elif el['type'] == 'way': 
                        ways.append(el)

                # 1. Process Relations: Flatten each member way into its own record
                for rel in relations:
                    relation_name = rel.get('tags', {}).get('name', '')
                    
                    for member in rel.get('members', []):
                        if member['type'] == 'way':
                            way_ref = member['ref']
                            
                            # Skip if this specific way segment was already saved
                            if way_ref in seen_way_ids:
                                continue
                                
                            if 'geometry' in member:
                                coords = [[pt['lon'], pt['lat']] for pt in member['geometry']]
                                
                                if coords:
                                    extracted_docks.append({
                                        'port_id': f"way_{way_ref}",
                                        'name': relation_name, # Inherits parent relation name
                                        'geometry_json': json.dumps(coords)
                                    })
                                    seen_way_ids.add(way_ref)

                # 2. Process Standalone Ways
                for way in ways:
                    way_id = way['id']
                    
                    # Deduplication: Skip if this way was already caught inside a relation
                    if way_id in seen_way_ids:
                        continue
                    
                    if 'geometry' in way:
                        coords = [[pt['lon'], pt['lat']] for pt in way['geometry']]
                        
                        if coords:
                            name = way.get('tags', {}).get('name', '')
                            extracted_docks.append({
                                'port_id': f"way_{way_id}",
                                'name': name,
                                'geometry_json': json.dumps(coords)
                            })
                            seen_way_ids.add(way_id)
                            
                # If we reached this point, the zone was processed successfully
                success = True
                print(f"Zone {i+1} completed successfully.")
                time.sleep(3) # Respect public server capacity between successful zones
                
            except Exception as e:
                attempt += 1
                print(f"** Failed on Zone {i+1} (Attempt {attempt}/{MAX_RETRIES}): {e} **")
                
                if attempt < MAX_RETRIES:
                    print(f"Waiting {RETRY_DELAY_SECONDS} seconds before retrying...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    print(f"CRITICAL: Zone {i+1} failed completely after {MAX_RETRIES} attempts. Skipping to next zone.")
    
    # 3. Write completely flat records to CSV
    print(f"\nWriting {len(extracted_docks)} flat records to {OUTPUT_CSV}...")
    
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['port_id', 'name', 'geometry_json']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        writer.writeheader()
        for dock in extracted_docks:
            writer.writerow(dock)
            
    print("Success. Every row is now a uniform, single-level geometry record.")

if __name__ == "__main__":
    main()