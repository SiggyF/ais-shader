
import logging
import sys
from pathlib import Path
import click
import rioxarray
import xarray as xr
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def create_transparent_cmap(base_cmap_name="viridis", min_alpha=0.0, max_alpha=1.0):
    """
    Create a colormap with a gradual alpha channel.
    """
    # Get base colormap
    if isinstance(base_cmap_name, list):
        # Custom list of colors
        base_cmap = mcolors.LinearSegmentedColormap.from_list("custom", base_cmap_name)
    else:
        base_cmap = plt.get_cmap(base_cmap_name)

    # Create new colormap with alpha
    n_colors = 256
    colors = base_cmap(np.linspace(0, 1, n_colors))
    
    # Modify alpha channel (linear gradient)
    alphas = np.linspace(min_alpha, max_alpha, n_colors)
    colors[:, 3] = alphas
    
    return mcolors.ListedColormap(colors)

def render_tile(nc_path, output_path, cmap, global_max, log_scale=True):
    """
    Render a single NetCDF to PNG using global scaling and custom colormap.
    """
    try:
        # Open NetCDF
        da = xr.open_dataarray(nc_path)
        
        # Sum bands if multi-band (for visualization)
        if da.shape[0] > 1:
            data = da.sum(dim="band").values
        else:
            data = da.values[0]
            
        # Normalize
        if log_scale:
            # Log scale: log(1 + x) / log(1 + max)
            norm_data = np.log1p(data) / np.log1p(global_max)
        else:
            norm_data = data / global_max
            
        # Clip to 0-1
        norm_data = np.clip(norm_data, 0, 1)
        
        # Apply colormap
        # cmap expects 0-1 input and returns RGBA (0-1)
        rgba = cmap(norm_data)
        
        # Convert to 0-255 uint8
        img_data = (rgba * 255).astype(np.uint8)
        
        # Save as PNG
        img = Image.fromarray(img_data, "RGBA")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(output_path)
        
    except Exception as e:
        logger.error(f"Failed to render {nc_path}: {e}")

def process_zoom_level(run_dir, zoom, cmap, global_max):
    """
    Process all NetCDFs for a specific zoom level.
    """
    tiff_dir = run_dir / "tiff"
    png_dir = run_dir / "png"
    
    # Find all NetCDFs for this zoom
    ncs = list(tiff_dir.glob(f"tile_{zoom}_*_counts.nc"))
    logger.info(f"Found {len(ncs)} tiles for Zoom {zoom}")
    
    if not ncs:
        return

    # Render in parallel
    with ProcessPoolExecutor() as executor:
        futures = []
        for nc in ncs:
            # Parse x, y from filename: tile_z_x_y_counts.nc
            parts = nc.name.split("_")
            x, y = parts[2], parts[3]
            png_path = png_dir / str(zoom) / x / f"{y}.png"
            
            futures.append(executor.submit(render_tile, nc, png_path, cmap, global_max))
            
        for _ in tqdm(futures, desc=f"Rendering Zoom {zoom}"):
            _.result()

def generate_pyramid(run_dir, base_zoom, cmap, global_max):
    """
    Generate lower zoom levels by aggregating upper levels.
    """
    tiff_dir = run_dir / "tiff"
    
    # Iterate from base_zoom - 1 down to 0
    for z in range(base_zoom - 1, -1, -1):
        logger.info(f"Generating Zoom {z}...")
        
        # We need to know which tiles to generate.
        # We can scan the z+1 directory to find existing children.
        child_ncs = list(tiff_dir.glob(f"tile_{z+1}_*_counts.nc"))
        
        # Group children by parent (z, x, y)
        # Parent of (z+1, x, y) is (z, x//2, y//2)
        parents = {}
        for child in child_ncs:
            parts = child.name.split("_")
            cx, cy = int(parts[2]), int(parts[3])
            px, py = cx // 2, cy // 2
            parent_key = (z, px, py)
            if parent_key not in parents:
                parents[parent_key] = []
            parents[parent_key].append(child)
            
        # Process each parent
        for (z, px, py), children in tqdm(parents.items(), desc=f"Zoom {z}"):
            # Initialize parent array (bands, height, width)
            # We need to know number of bands from first child
            with xr.open_dataarray(children[0]) as src:
                n_bands = src.shape[0] if len(src.shape) > 2 else 1
                dtype = src.dtype
                # crs = src.rio.crs # NetCDF might not have rio accessor ready without decode_coords="all"
                
            # Create empty parent
            tile_size = 1024
            parent_data = np.zeros((n_bands, tile_size, tile_size), dtype=dtype)
            
            for child_path in children:
                parts = child_path.name.split("_")
                cx, cy = int(parts[2]), int(parts[3])
                
                # Determine quadrant
                is_right = cx % 2
                is_bottom = cy % 2
                
                # Target slice in parent
                half_size = tile_size // 2
                x_start = is_right * half_size
                x_end = x_start + half_size
                y_start = is_bottom * half_size
                y_end = y_start + half_size
                
                with xr.open_dataarray(child_path) as child:
                    # Coarsen
                    # child is (band, y, x)
                    coarsened = child.coarsen(y=2, x=2, boundary="trim").sum()
                    
                    # Place in parent
                    parent_data[:, y_start:y_end, x_start:x_end] = coarsened.values
            
            # Save Parent NetCDF
            parent_nc_path = tiff_dir / f"tile_{z}_{px}_{py}_counts.nc"
            
            da_parent = xr.DataArray(
                parent_data,
                dims=("band", "y", "x"),
                coords=None
            )
            # da_parent.rio.write_crs("EPSG:3857", inplace=True) # Optional for PNG step
            da_parent.to_netcdf(parent_nc_path)
            
            # Render Parent PNG
            png_path = run_dir / "png" / str(z) / str(px) / f"{py}.png"
            render_tile(parent_nc_path, png_path, cmap, global_max)

@click.command()
@click.option("--run-dir", type=click.Path(exists=True, path_type=Path), required=True, help="Path to the run directory.")
@click.option("--base-zoom", type=int, default=7, help="Base zoom level to render.")
def main(run_dir, base_zoom):
    """
    Post-process NetCDFs to PNGs with global scaling and transparency.
    """
    tiff_dir = run_dir / "tiff"
    
    # 1. Calculate Global Max
    logger.info("Calculating global max...")
    global_max = 0
    ncs = list(tiff_dir.glob(f"tile_{base_zoom}_*_counts.nc"))
    
    for nc in tqdm(ncs, desc="Scanning tiles"):
        with xr.open_dataarray(nc) as da:
            # Sum bands if needed
            if da.shape[0] > 1:
                val = da.sum(dim="band").max().item()
            else:
                val = da.max().item()
            if val > global_max:
                global_max = val
                
    logger.info(f"Global Max: {global_max}")
    
    # 2. Define Colormap (Electric Blue with Alpha)
    # Dark Blue -> Cyan -> White
    colors = ["#001133", "#0044aa", "#00aaff", "#00ffff", "#ffffff"]
    cmap = create_transparent_cmap(colors, min_alpha=0.0, max_alpha=1.0)
    
    # 3. Render Base Zoom
    process_zoom_level(run_dir, base_zoom, cmap, global_max)
    
    # 4. Generate Pyramid
    generate_pyramid(run_dir, base_zoom, cmap, global_max)

if __name__ == "__main__":
    main()
