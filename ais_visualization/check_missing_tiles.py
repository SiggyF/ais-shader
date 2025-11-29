
import dask_geopandas
import morecantile
import shapely.geometry

def check_missing():
    tms = morecantile.tms.get("WebMercatorQuad")
    path = "/Users/baart_f/data/ais/AISVesselTracks2023_processed.parquet"
    print(f"Loading {path}...")
    ddf = dask_geopandas.read_parquet(path)
    
    # Check Tile 5/6/12 (Random Y in US latitude)
    # US Lat is roughly 24 to 49.
    # At z=5:
    # y=10 is ~ 50 deg
    # y=11 is ~ 40 deg
    # y=12 is ~ 30 deg
    # y=13 is ~ 20 deg
    
    tiles_to_check = [
        morecantile.Tile(x=6, y=11, z=5),
        morecantile.Tile(x=6, y=12, z=5),
        morecantile.Tile(x=7, y=11, z=5),
        morecantile.Tile(x=7, y=12, z=5),
    ]
    
    for tile in tiles_to_check:
        bbox = tms.xy_bounds(tile)
        print(f"Checking Tile {tile} Bounds: {bbox}")
        
        # Filter
        subset = ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
        
        # Count
        # We use compute() on len to be sure
        count = len(subset)
        print(f"Tile {tile} has {count} tracks.")

if __name__ == "__main__":
    check_missing()
