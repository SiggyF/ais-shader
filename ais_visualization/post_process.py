import logging
import sys
from pathlib import Path
from collections import defaultdict
import xarray as xr
import numpy as np
import rioxarray
from tqdm import tqdm
import datashader.transfer_functions as tf
from datashader.colors import viridis
from dask.distributed import Client, as_completed
import dask
from concurrent.futures import ProcessPoolExecutor
import click
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

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
    # Open Zarr
    # Use open_zarr
    with xr.open_zarr(nc_path) as ds:
        # The variable name is likely __xarray_dataarray_variable__ or similar
        # We can find the data variable by checking data_vars
        # Find the correct data variable
        var_name = None
        if "counts" in ds.data_vars:
            var_name = "counts"
        elif "__xarray_dataarray_variable__" in ds.data_vars:
            var_name = "__xarray_dataarray_variable__"
        else:
            # Fallback: pick first non-spatial_ref variable
            for v in ds.data_vars:
                if v != "spatial_ref":
                    var_name = v
                    break
        
        if not var_name:
            logger.warning(f"No data variable found in {nc_path}")
            return

        da = ds[var_name]
        
        # Sum over extra dimensions to get 2D (y, x) for visualization
        # Dims might be (band, y, x, VesselGroup) or similar
        dims_to_sum = [d for d in da.dims if d not in ('y', 'x')]
        
        if dims_to_sum:
            data = da.sum(dim=dims_to_sum).values
        else:
            data = da.values
        
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
    
    # Flip vertically because Datashader/NetCDF y is ascending (bottom-to-top)
    # but PIL expects top-to-bottom
    img_data = np.flipud(img_data)
    
    # Save as PNG
    img = Image.fromarray(img_data, "RGBA")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(output_path)

def process_zoom_level(run_dir, zoom, cmap, global_max, client):
    """
    Process all NetCDFs for a specific zoom level.
    """
    nc_dir = run_dir / "zarr"
    png_dir = run_dir / "png"
    
    # Find all Zarrs for this zoom
    # DirectoryStore zarrs are directories, so glob works but we need to check if it's a dir
    # Actually glob returns paths, we can just use them
    ncs = list(nc_dir.glob(f"tile_{zoom}_*.zarr"))
    logger.info(f"Found {len(ncs)} tiles for Zoom {zoom}")
    
    if not ncs:
        return

    # Render in parallel using Dask
    from dask.distributed import as_completed
    
    futures = []
    for nc in ncs:
        parts = nc.name.split("_")
        x, y = parts[2], parts[3]
        png_path = png_dir / str(zoom) / x / f"{y}.png"
        futures.append(client.submit(render_tile, nc, png_path, cmap, global_max))
    
    for _ in tqdm(as_completed(futures), total=len(futures), desc=f"Rendering Zoom {zoom}"):
        pass

def generate_pyramid(run_dir, base_zoom, cmap, global_max, client):
    """
    Generate lower zoom levels by aggregating upper levels.
    """
    nc_dir = run_dir / "zarr"
    
    # Iterate from base_zoom - 1 down to 0 (must be sequential)
    for z in range(base_zoom - 1, -1, -1):
        logger.info(f"Generating Zoom {z}...")
        
        child_ncs = list(nc_dir.glob(f"tile_{z+1}_*.zarr"))
        
        parents = defaultdict(list)
        for child in child_ncs:
            parts = child.name.split("_")
            # parts[1] is the zoom level, parts[2] is x, parts[3] is y
            cx, cy = int(parts[2]), int(parts[3])
            px, py = cx // 2, cy // 2
            parent_key = (z, px, py) # Parent zoom level is 'z'
            parents[parent_key].append(child)
            
        logger.info(f"Zoom {z}: Found {len(child_ncs)} child tiles, grouped into {len(parents)} parent tiles.")
        if len(parents) > 0:
            first_parent_key = list(parents.keys())[0]
            logger.info(f"Sample parent {first_parent_key} has {len(parents[first_parent_key])} children: {[p.name for p in parents[first_parent_key]]}")
            
        # Process parents in parallel
        from dask.distributed import as_completed
        futures = []
        for parent_key, children in parents.items():
            futures.append(client.submit(process_parent_tile, parent_key, children, nc_dir, run_dir, cmap, global_max))
        
        for _ in tqdm(as_completed(futures), total=len(futures), desc=f"Zoom {z}"):
            pass

def export_cogs(run_dir, base_zoom, client):
    """
    Convert NetCDF tiles at base_zoom to Cloud Optimized GeoTIFFs.
    """
    nc_dir = run_dir / "zarr"
    tiff_dir = run_dir / "tiff"
    tiff_dir.mkdir(parents=True, exist_ok=True)
    
    ncs = list(nc_dir.glob(f"tile_{base_zoom}_*.zarr"))
    logger.info(f"Exporting {len(ncs)} COGs for Zoom {base_zoom}...")
    
    futures = [client.submit(export_single_cog, nc, tiff_dir) for nc in ncs]
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Exporting COGs"):
        pass

def aggregate_children(parent_key, children):
    """
    Aggregate child NetCDF files into a single parent DataArray.
    """
    z, px, py = parent_key
    child_ds_list = []
    all_categories = set()
    
    for child_path in children:
        # Use open_zarr
        with xr.open_zarr(child_path) as ds:
            # Assume "counts" variable or fallback to first non-spatial
            if "counts" in ds.data_vars:
                da = ds["counts"]
            else:
                # Fallback: pick first non-spatial_ref variable
                # This assumes the structure is consistent across files
                vars = [v for v in ds.data_vars if v != "spatial_ref"]
                if not vars:
                    logger.warning(f"Skipping {child_path}: No data variable found.")
                    continue
                da = ds[vars[0]]

            # Load data into memory to avoid file handle issues during aggregation
            da.load()
            child_ds_list.append((child_path, da))
            
            non_spatial_dims = [d for d in da.dims if d not in ('y', 'x', 'band')]
            if non_spatial_dims:
                cat_dim = non_spatial_dims[0]
                cats = da.coords[cat_dim].values
                all_categories.update(cats)
    
    if not child_ds_list:
        return None

    # Sort categories for consistency
    sorted_categories = sorted(list(all_categories))
    
    # Create Parent DataArray
    tile_size = 1024
    template_da = child_ds_list[0][1]
    parent_dims = list(template_da.dims)
    parent_coords = dict(template_da.coords)
    
    if non_spatial_dims:
        cat_dim = non_spatial_dims[0]
        parent_coords[cat_dim] = sorted_categories
        
        parent_shape = []
        for d in parent_dims:
            if d == 'y': parent_shape.append(tile_size)
            elif d == 'x': parent_shape.append(tile_size)
            elif d == cat_dim: parent_shape.append(len(sorted_categories))
            else: parent_shape.append(template_da.sizes[d])
    else:
        parent_shape = [template_da.sizes[d] if d not in ('y', 'x') else tile_size for d in parent_dims]

    parent_data = np.zeros(parent_shape, dtype=template_da.dtype)
    
    # Fill Parent
    for child_path, da_child in child_ds_list:
        parts = child_path.name.split("_")
        cx, cy = int(parts[2]), int(parts[3])
        
        # Determine quadrant
        is_right = cx % 2
        is_bottom = cy % 2
        
        # Invert Y-slice logic to construct a Bottom-up array (NetCDF standard)
        y_slice = slice((1 - is_bottom) * 512, (2 - is_bottom) * 512)
        x_slice = slice(is_right * 512, (is_right + 1) * 512)
        
        if non_spatial_dims:
            cat_dim = non_spatial_dims[0]
            da_child_aligned = da_child.reindex({cat_dim: sorted_categories}, fill_value=0)
        else:
            da_child_aligned = da_child
        
        coarsened = da_child_aligned.coarsen(y=2, x=2, boundary="trim").sum()
        
        np_slices = []
        for d in parent_dims:
            if d == 'y': np_slices.append(y_slice)
            elif d == 'x': np_slices.append(x_slice)
            else: np_slices.append(slice(None))
        
        parent_data[tuple(np_slices)] = coarsened.values

    da_parent = xr.DataArray(parent_data, dims=parent_dims, coords=parent_coords)
    da_parent.name = "counts"
    return da_parent

def save_zarr(da, path):
    """
    Save DataArray to Zarr with compression.
    """
    # Zarr handles compression automatically (Blosc default)
    da = da.astype("int32")
    da.to_zarr(path, mode="w", consolidated=True)
    logger.info(f"Saved parent Zarr: {path}")

def process_parent_tile(parent_key, children, nc_dir, run_dir, cmap, global_max):
    """
    Process a single parent tile: aggregate children, save Zarr, render PNG.
    """
    z, px, py = parent_key
    logger.info(f"Processing parent {parent_key} with {len(children)} children")
    
    # 1. Aggregate
    da_parent = aggregate_children(parent_key, children)
    if da_parent is None:
        logger.warning(f"No children processed for parent {parent_key}")
        return

    # 2. Save Zarr
    parent_nc_path = nc_dir / f"tile_{z}_{px}_{py}.zarr"
    save_zarr(da_parent, parent_nc_path)
    
    # 3. Render PNG
    png_path = run_dir / "png" / str(z) / str(px) / f"{py}.png"
    render_tile(parent_nc_path, png_path, cmap, global_max)



def export_single_cog(nc_path, tiff_dir):
    """
    Convert a single NetCDF tile to COG.
    """
    try:
        with xr.open_zarr(nc_path) as ds:
            # Find the correct data variable
            var_name = None
            if "counts" in ds.data_vars:
                var_name = "counts"
            elif "__xarray_dataarray_variable__" in ds.data_vars:
                var_name = "__xarray_dataarray_variable__"
            else:
                # Fallback: pick first non-spatial_ref variable
                for v in ds.data_vars:
                    if v != "spatial_ref":
                        var_name = v
                        break
            
            if not var_name:
                return None

            da = ds[var_name]
            
            # Prepare for GeoTIFF: needs (band, y, x)
            non_spatial = [d for d in da.dims if d not in ('y', 'x')]
            
            if len(non_spatial) > 1:
                if 'band' in da.dims and da.sizes['band'] == 1:
                    da = da.squeeze('band')
                
                cat_dim = [d for d in da.dims if d not in ('y', 'x')][0]
                da = da.transpose(cat_dim, 'y', 'x')
                
            elif len(non_spatial) == 1:
                d = non_spatial[0]
                da = da.transpose(d, 'y', 'x')
            
            # Ensure float/int type compatible with TIFF
            da = da.astype("float32")
            
            if not da.rio.crs:
                da.rio.write_crs("EPSG:3857", inplace=True)
                
            tiff_path = tiff_dir / nc_path.name.replace(".nc", ".tif")
            
            # Save as COG
            da.rio.to_raster(tiff_path, tiled=True, compress="DEFLATE")
            return tiff_path
            
    except Exception as e:
        logger.warning(f"Failed to export COG {nc_path}: {e}")
        return None



@click.command()
@click.option("--run-dir", type=click.Path(exists=True, path_type=Path), required=True, help="Path to the run directory.")
@click.option("--base-zoom", type=int, default=7, help="Base zoom level to render.")
@click.option("--scheduler", type=str, default=None, help="Address of the Dask scheduler (e.g., tcp://127.0.0.1:8786).")
@click.option("--clean-intermediate", is_flag=True, help="Delete intermediate NetCDF files (Zoom 0 to base-zoom-1) after processing.")
@click.option("--cogs", is_flag=True, help="Export Cloud Optimized GeoTIFFs for the base zoom level.")
def main(run_dir, base_zoom, scheduler, clean_intermediate, cogs):
    """
    Post-process NetCDFs to PNGs with global scaling and transparency.
    """
    # Initialize Dask Client
    
    if scheduler:
        client = Client(scheduler)
        logger.info(f"Connected to Dask scheduler at {scheduler}")
    else:
        client = Client() # Local cluster
        logger.info(f"Started local Dask cluster: {client.dashboard_link}")

    nc_dir = run_dir / "zarr"
    
    # 1. Calculate Global Max
    logger.info("Calculating global max...")
    global_max = 0
    ncs = list(nc_dir.glob(f"tile_{base_zoom}_*.zarr"))
    
    for nc in tqdm(ncs, desc="Scanning tiles"):
        with xr.open_zarr(nc) as ds:
            # Find the correct data variable
            var_name = None
            if "counts" in ds.data_vars:
                var_name = "counts"
            elif "__xarray_dataarray_variable__" in ds.data_vars:
                var_name = "__xarray_dataarray_variable__"
            else:
                # Fallback: pick first non-spatial_ref variable
                for v in ds.data_vars:
                    if v != "spatial_ref":
                        var_name = v
                        break
            
            if not var_name:
                continue

            da = ds[var_name]
            
            dims_to_sum = [d for d in da.dims if d not in ('y', 'x')]
            if dims_to_sum:
                val = da.sum(dim=dims_to_sum).max().item()
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
    process_zoom_level(run_dir, base_zoom, cmap, global_max, client)
    
    # 4. Generate Pyramid
    generate_pyramid(run_dir, base_zoom, cmap, global_max, client)
    
    # 5. Export COGs
    if cogs:
        export_cogs(run_dir, base_zoom, client)

    # 6. Cleanup Intermediate Files
    if clean_intermediate:
        logger.info("Cleaning up intermediate Zarr files...")
        import shutil
        # Delete all .zarr files where zoom < base_zoom
        for nc in nc_dir.glob("tile_*.zarr"):
            try:
                parts = nc.name.split("_")
                z = int(parts[1])
                if z < base_zoom:
                    shutil.rmtree(nc)
            except Exception as e:
                logger.warning(f"Failed to delete {nc}: {e}")
        logger.info("Cleanup complete.")

if __name__ == "__main__":
    main()
