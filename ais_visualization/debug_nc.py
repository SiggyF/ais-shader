
import xarray as xr
import sys

def inspect(path):
    print(f"Inspecting {path}...")
    try:
        ds = xr.open_dataset(path)
        print("Dataset:")
        print(ds)
        print("\nVariables:")
        for v in ds.variables:
            print(f" - {v}: {ds[v].dims}, {ds[v].shape}")
            
    except Exception as e:
        print(f"Error opening as dataset: {e}")

if __name__ == "__main__":
    inspect("rendered/run_20251129_220323/tiff/tile_7_31_44_counts.nc")
