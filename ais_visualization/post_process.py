
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
    # Open NetCDF
    # Use open_dataset because open_dataarray fails with multiple vars (spatial_ref)
    with xr.open_dataset(nc_path) as ds:
        # The variable name is likely __xarray_dataarray_variable__ or similar
        # We can find the data variable by checking data_vars
        var_name = list(ds.data_vars)[0]
        if "__xarray_dataarray_variable__" in ds.data_vars:
            var_name = "__xarray_dataarray_variable__"
        
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
    
    # Save as PNG
    img = Image.fromarray(img_data, "RGBA")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(output_path)

def process_zoom_level(run_dir, zoom, cmap, global_max):
    """
    Process all NetCDFs for a specific zoom level.
    """
    nc_dir = run_dir / "nc"
    png_dir = run_dir / "png"
    
    # Find all NetCDFs for this zoom
    ncs = list(nc_dir.glob(f"tile_{zoom}_*_counts.nc"))
    logger.info(f"Found {len(ncs)} tiles for Zoom {zoom}")
    
    if not ncs:
        return

    if not ncs:
        return

    # Render in parallel using Dask
    from dask.distributed import get_client, as_completed
    try:
        client = get_client()
        logger.info(f"Using Dask client: {client}")
    except ValueError:
        logger.warning("No Dask client found. Falling back to serial execution.")
        client = None

    if client:
        futures = []
        for nc in ncs:
            parts = nc.name.split("_")
            x, y = parts[2], parts[3]
            png_path = png_dir / str(zoom) / x / f"{y}.png"
            futures.append(client.submit(render_tile, nc, png_path, cmap, global_max))
        
        for _ in tqdm(as_completed(futures), total=len(futures), desc=f"Rendering Zoom {zoom}"):
            pass
    else:
        # Serial fallback (or ProcessPoolExecutor if we wanted, but let's stick to Dask or Serial)
        for nc in tqdm(ncs, desc=f"Rendering Zoom {zoom}"):
            parts = nc.name.split("_")
            x, y = parts[2], parts[3]
            png_path = png_dir / str(zoom) / x / f"{y}.png"
            render_tile(nc, png_path, cmap, global_max)

def generate_pyramid(run_dir, base_zoom, cmap, global_max):
    """
    Generate lower zoom levels by aggregating upper levels.
    """
    nc_dir = run_dir / "nc"
    
    # Iterate from base_zoom - 1 down to 0
    for z in range(base_zoom - 1, -1, -1):
        logger.info(f"Generating Zoom {z}...")
        
        # We need to know which tiles to generate.
        # We can scan the z+1 directory to find existing children.
        child_ncs = list(nc_dir.glob(f"tile_{z+1}_*_counts.nc"))
        
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
            # We need to handle inconsistent categories (variables) across children.
            # 1. Read all children to find union of categories
            child_ds_list = []
            all_categories = set()
            
            for child_path in children:
                try:
                    ds = xr.open_dataset(child_path)
                    # Identify the data variable
                    var_name = list(ds.data_vars)[0]
                    if "__xarray_dataarray_variable__" in ds.data_vars:
                        var_name = "__xarray_dataarray_variable__"
                    
                    da = ds[var_name]
                    child_ds_list.append((child_path, da))
                    
                    # Check categories (last dim usually)
                    # Dims: (band, y, x, VesselGroup)
                    # We assume the last dim is the category dim if it's not spatial/band
                    # Or we can just look at the coords of the last dim
                    non_spatial_dims = [d for d in da.dims if d not in ('y', 'x', 'band')]
                    if non_spatial_dims:
                        cat_dim = non_spatial_dims[0]
                        cats = da.coords[cat_dim].values
                        all_categories.update(cats)
                except Exception as e:
                    logger.warning(f"Failed to read {child_path}: {e}")
            
            if not child_ds_list:
                continue

            # Sort categories for consistency
            sorted_categories = sorted(list(all_categories))
            
            # 2. Create Parent DataArray
            # Shape: (band, y, x, categories)
            # We assume band=1 for now as per previous logic, or take max bands
            n_bands = 1
            tile_size = 1024
            
            # We need to construct the shape dynamically
            # (band, y, x, cat_dim)
            # But we need to know the order.
            # Let's use the first child as a template for dim order, but replace sizes
            template_da = child_ds_list[0][1]
            parent_dims = list(template_da.dims)
            parent_coords = dict(template_da.coords)
            
            # Update category coord
            if non_spatial_dims:
                cat_dim = non_spatial_dims[0]
                parent_coords[cat_dim] = sorted_categories
                
                # Update shape
                parent_shape = []
                for d in parent_dims:
                    if d == 'y': parent_shape.append(tile_size)
                    elif d == 'x': parent_shape.append(tile_size)
                    elif d == cat_dim: parent_shape.append(len(sorted_categories))
                    else: parent_shape.append(template_da.sizes[d])
            else:
                # No categories (just band, y, x)
                parent_shape = [template_da.sizes[d] if d not in ('y', 'x') else tile_size for d in parent_dims]

            parent_data = np.zeros(parent_shape, dtype=template_da.dtype)
            
            # Create Parent DataArray (empty)
            da_parent = xr.DataArray(parent_data, dims=parent_dims, coords=parent_coords)
            
            # 3. Fill Parent
            for child_path, da_child in child_ds_list:
                parts = child_path.name.split("_")
                cx, cy = int(parts[2]), int(parts[3])
                
                # Determine quadrant
                is_right = cx % 2
                is_bottom = cy % 2
                
                # Target slice
                y_slice = slice(is_bottom * 512, (is_bottom + 1) * 512)
                x_slice = slice(is_right * 512, (is_right + 1) * 512)
                
                # Reindex child to match parent categories
                if non_spatial_dims:
                    cat_dim = non_spatial_dims[0]
                    # fill_value=0 for missing categories
                    da_child_aligned = da_child.reindex({cat_dim: sorted_categories}, fill_value=0)
                else:
                    da_child_aligned = da_child
                
                # Coarsen
                coarsened = da_child_aligned.coarsen(y=2, x=2, boundary="trim").sum()
                
                # Assign to parent
                # We need to use loc or isel, but since we have slices for y/x...
                # We can construct a selector
                selector = {d: slice(None) for d in parent_dims}
                selector['y'] = y_slice
                selector['x'] = x_slice
                
                # Assign values
                # Note: da_parent[selector] might not work directly for assignment with complex dims
                # Using numpy assignment on .values is safer if we match the shape
                
                # We need to ensure coarsened has same shape as the target slice
                # It should, because we reindexed categories.
                
                # However, converting to numpy loses dimension order safety.
                # Let's trust the order is preserved since we used the same dims.
                
                # We need to map the selector to numpy slices
                np_slices = []
                for d in parent_dims:
                    if d == 'y': np_slices.append(y_slice)
                    elif d == 'x': np_slices.append(x_slice)
                    else: np_slices.append(slice(None))
                
                parent_data[tuple(np_slices)] = coarsened.values

            # Update da_parent with filled data
            da_parent.values = parent_data
            
            # Save Parent NetCDF
            parent_nc_path = nc_dir / f"tile_{z}_{px}_{py}_counts.nc"
            da_parent.to_netcdf(parent_nc_path)
            
            # Render Parent PNG
            png_path = run_dir / "png" / str(z) / str(px) / f"{py}.png"
            render_tile(parent_nc_path, png_path, cmap, global_max)

@click.command()
@click.option("--run-dir", type=click.Path(exists=True, path_type=Path), required=True, help="Path to the run directory.")
@click.option("--base-zoom", type=int, default=7, help="Base zoom level to render.")
@click.option("--scheduler", type=str, default=None, help="Address of the Dask scheduler (e.g., tcp://127.0.0.1:8786).")
def main(run_dir, base_zoom, scheduler):
    """
    Post-process NetCDFs to PNGs with global scaling and transparency.
    """
    # Initialize Dask Client
    from dask.distributed import Client
    if scheduler:
        client = Client(scheduler)
        logger.info(f"Connected to Dask scheduler at {scheduler}")
    else:
        client = Client() # Local cluster
        logger.info(f"Started local Dask cluster: {client.dashboard_link}")

    nc_dir = run_dir / "nc"
    
    # 1. Calculate Global Max
    logger.info("Calculating global max...")
    global_max = 0
    ncs = list(nc_dir.glob(f"tile_{base_zoom}_*_counts.nc"))
    
    for nc in tqdm(ncs, desc="Scanning tiles"):
        try:
            with xr.open_dataset(nc) as ds:
                var_name = list(ds.data_vars)[0]
                if "__xarray_dataarray_variable__" in ds.data_vars:
                    var_name = "__xarray_dataarray_variable__"
                da = ds[var_name]
                
                dims_to_sum = [d for d in da.dims if d not in ('y', 'x')]
                if dims_to_sum:
                    val = da.sum(dim=dims_to_sum).max().item()
                else:
                    val = da.max().item()
                    
                if val > global_max:
                    global_max = val
        except Exception as e:
            logger.warning(f"Skipping {nc}: {e}")
                
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
