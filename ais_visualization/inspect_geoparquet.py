
import geopandas as gpd
import pyarrow.parquet as pq
import json

def inspect_file(path):
    print(f"Inspecting {path}...")
    try:
        # Check Parquet metadata
        table = pq.read_metadata(path)
        metadata = table.metadata
        print("Parquet Metadata keys:", metadata.keys())
        
        if b'geo' in metadata:
            print("Found 'geo' metadata!")
            geo_meta = json.loads(metadata[b'geo'])
            print(json.dumps(geo_meta, indent=2))
        else:
            print("No 'geo' metadata found.")
            
        # Try reading with geopandas
        print("\nAttempting gpd.read_parquet...")
        gdf = gpd.read_parquet(path)
        print(f"Success! CRS: {gdf.crs}")
        print(gdf.head())
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_file("/Users/baart_f/data/ais/AISVesselTracks2024_raw.parquet")
