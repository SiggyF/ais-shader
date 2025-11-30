import logging
import sys
from pathlib import Path
import xarray as xr
import rioxarray
from post_process import export_single_cog

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_band_tags():
    # Use a tile from the previous run
    run_dir = Path("rendered/run_20251130_132624")
    zarr_path = run_dir / "zarr/tile_7_37_48.zarr"
    
    if not zarr_path.exists():
        print(f"Test tile not found: {zarr_path}")
        return

    output_dir = Path("test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Inspect Zarr
    with xr.open_zarr(zarr_path) as ds:
        print("Zarr Dataset:", ds)
        if "counts" in ds:
            print("counts coords:", ds["counts"].coords)
    
    print(f"Exporting {zarr_path} to {output_dir}...")
    tiff_path = export_single_cog(zarr_path, output_dir)
    
    if tiff_path:
        print(f"Exported to {tiff_path}")
        import rasterio
        with rasterio.open(tiff_path) as src:
            print("Tags:", src.tags())
            print("Descriptions:", src.descriptions)
            for i in range(1, src.count + 1):
                print(f"Band {i} tags:", src.tags(i))
    else:
        print("Export failed.")

if __name__ == "__main__":
    test_band_tags()
