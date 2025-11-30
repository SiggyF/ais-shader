import logging
import datashader as ds
import datashader.transfer_functions as tf
from datashader.utils import export_image
import morecantile
import rioxarray
import xarray as xr
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from pathlib import Path
from rasterio.transform import from_bounds
from datashader.transfer_functions import shade
from datashader.colors import viridis
from datashader.colors import viridis

logger = logging.getLogger(__name__)

def render_tile_task(gdf_local, tile, zarr_dir, config):
    """
    Render a single tile from a computed GeoDataFrame.
    This runs on a worker.
    """
    if len(gdf_local) == 0:
        # logger.info(f"Tile {tile} is empty. Skipping.")
        return

    tms = morecantile.tms.get("WebMercatorQuad")
    bbox = tms.xy_bounds(tile)
    
    # Define canvas
    tile_size = config["visualization"]["tile_size"]
    cvs = ds.Canvas(
        plot_width=tile_size, 
        plot_height=tile_size,
        x_range=(bbox.left, bbox.right),
        y_range=(bbox.bottom, bbox.top)
    )

    # Aggregate
    line_width = config["visualization"]["line_width"]
    category_column = config["visualization"].get("category_column")
    
    if category_column:
        if category_column in gdf_local.columns:
            gdf_local[category_column] = gdf_local[category_column].astype("category")
            agg = cvs.line(gdf_local, geometry='geometry', agg=ds.by(category_column, ds.count()))
        else:
            logger.warning(f"Category column '{category_column}' not found. Falling back to simple count.")
            agg = cvs.line(gdf_local, geometry='geometry', agg=ds.count())
    elif line_width == 0:
        agg = cvs.line(gdf_local, geometry='geometry', agg=ds.count())
    else:
        agg = cvs.line(gdf_local, geometry='geometry', line_width=line_width)

    # --- Save Zarr (Counts) ---
    # Create transform
    transform = from_bounds(bbox.left, bbox.bottom, bbox.right, bbox.top, tile_size, tile_size)
    
    # Prepare DataArray for saving
    if isinstance(agg, xr.Dataset):
        da = agg.to_array(dim="band")
        da = da.fillna(0).astype("int32")
    else:
        da = agg.fillna(0).astype("int32")
        da = da.expand_dims(dim={'band': 1})

    # Set CRS and Transform
    da.rio.write_crs("EPSG:3857", inplace=True)
    da.rio.write_transform(transform, inplace=True)
    
    # Save as Zarr
    zarr_path = zarr_dir / f"tile_{tile.z}_{tile.x}_{tile.y}.zarr"
    
    if not da.name:
        da.name = "counts"
    
    # Encoding: disable compression for spatial_ref
    encoding = {"spatial_ref": {"compressor": None}}
    
    da.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)
    
    # Logging stats
    if isinstance(agg, xr.Dataset):
        total_sum = float(da.sum())
        max_val = float(da.max())
        logger.info(f"Tile {tile} stats: sum={total_sum}, max={max_val}, categories={len(agg.data_vars)}")
    else:
        agg_sum = float(agg.sum())
        agg_max = float(agg.max())
        logger.info(f"Tile {tile} stats: sum={agg_sum}, max={agg_max}")

    logger.info(f"Saved Zarr for tile {tile}")


def render_tiles(coords_ddf, output_dir: Path, config: dict):
    """
    Render tiles for the US using Datashader and save as Zarr.
    Submits all tasks to Dask at once.
    """
    from dask.distributed import get_client, wait
    
    try:
        client = get_client()
    except ValueError:
        logger.error("No Dask client found. Please start a client before calling render_tiles.")
        return

    # Define TileMatrixSet (WebMercatorQuad)
    tms = morecantile.tms.get("WebMercatorQuad")
    
    # Define US Bounding Box
    us_bbox = tuple(config["visualization"]["bbox"])
    zoom = config["visualization"]["zoom"]
    
    logger.info(f"Generating tiles for BBox {us_bbox} at Zoom {zoom}...")
    
    tiles = list(tms.tiles(*us_bbox, zooms=[zoom]))
    
    # Create subdirectories
    zarr_dir = output_dir / "zarr"
    zarr_dir.mkdir(parents=True, exist_ok=True)
    
    # Process tiles
    total_tiles = len(tiles)
    logger.info(f"Found {total_tiles} tiles to render. Submitting tasks...")
    
    futures = []
    for tile in tiles:
        # Check if output already exists
        zarr_path = zarr_dir / f"tile_{tile.z}_{tile.x}_{tile.y}.zarr"
        if zarr_path.exists():
            # logger.info(f"Tile {tile} already exists. Skipping.")
            continue

        bbox = tms.xy_bounds(tile)
        
        # Filter data for this tile using spatial index (Lazy)
        subset = coords_ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
        
        # Submit the compute task (returns a Future to the pandas DataFrame)
        future_gdf = client.compute(subset)
        
        # Submit the rendering task, dependent on future_gdf
        future_render = client.submit(render_tile_task, future_gdf, tile, zarr_dir, config)
        futures.append(future_render)
    
    logger.info(f"Submitted {len(futures)} tasks. Waiting for completion...")
    wait(futures)
    logger.info("All tasks completed.")
