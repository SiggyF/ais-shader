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

logger = logging.getLogger(__name__)

def process_single_tile(tile, coords_ddf, png_dir: Path, tiff_dir: Path, config: dict):
    """
    Process a single tile: filter, compute, render, and save.
    """
    tms = morecantile.tms.get("WebMercatorQuad")
    bbox = tms.xy_bounds(tile)
    
    # Filter data for this tile using spatial index
    subset = coords_ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
    
    # Compute to local GeoDataFrame
    gdf_local = subset.compute()

    if len(gdf_local) == 0:
        logger.info(f"Tile {tile} is empty. Skipping.")
        return

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
    
    try:
        if category_column:
            # Categorical aggregation (count per category)
            # Ensure column is categorical for Datashader
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
            
    except ValueError as e:
        logger.warning(f"Rendering failed for tile {tile}: {e}")
        return

    # --- Save GeoTIFF (Counts) ---
    # Create transform
    transform = from_bounds(bbox.left, bbox.bottom, bbox.right, bbox.top, tile_size, tile_size)
    
    # Prepare DataArray for saving
    if isinstance(agg, xr.Dataset):
        # Multi-band (Categorical)
        da = agg.to_array(dim="band")
        da = da.fillna(0).astype("uint32")
    else:
        # Single band
        da = agg.fillna(0).astype("uint32")
        da = da.expand_dims(dim={'band': 1})

    # Set CRS and Transform
    da.rio.write_crs("EPSG:3857", inplace=True)
    da.rio.write_transform(transform, inplace=True)
    
    # Save as NetCDF (supports N-dims better than TIFF for intermediate)
    nc_path = nc_dir / f"tile_{tile.z}_{tile.x}_{tile.y}_counts.nc"
    da.to_netcdf(nc_path)
    
    # Logging stats
    if isinstance(agg, xr.Dataset):
        # Sum across all categories
        total_sum = float(da.sum())
        max_val = float(da.max())
        logger.info(f"Tile {tile} stats: sum={total_sum}, max={max_val}, categories={len(agg.data_vars)}")
    else:
        agg_sum = float(agg.sum())
        agg_max = float(agg.max())
        logger.info(f"Tile {tile} stats: sum={agg_sum}, max={agg_max}")

    logger.info(f"Saved NetCDF for tile {tile}")
    


    logger.info(f"Saved TIFF for tile {tile}")


def render_tiles(coords_ddf, output_dir: Path, config: dict):
    """
    Render tiles for the US using Datashader and save as PNG and COG.
    """
    # Define TileMatrixSet (WebMercatorQuad)
    tms = morecantile.tms.get("WebMercatorQuad")
    
    # Define US Bounding Box
    us_bbox = tuple(config["visualization"]["bbox"])
    zoom = config["visualization"]["zoom"]
    
    logger.info(f"Generating tiles for BBox {us_bbox} at Zoom {zoom}...")
    
    tiles = list(tms.tiles(*us_bbox, zooms=[zoom]))
    
    # Create subdirectories
    tiff_dir = output_dir / "tiff"
    tiff_dir.mkdir(parents=True, exist_ok=True)
    
    nc_dir = output_dir / "nc"
    nc_dir.mkdir(parents=True, exist_ok=True)
    
    png_dir = output_dir / "png"
    png_dir.mkdir(parents=True, exist_ok=True)
    
    # Process tiles
    # We can parallelize this, but Dask is already parallelizing the aggregation.
    # So we iterate and compute.
    
    total_tiles = len(tiles)
    logger.info(f"Found {total_tiles} tiles to render.")
    
    for i, tile in enumerate(tiles):
        logger.info(f"Processing Tile: {tile}...")
        process_single_tile(tile, coords_ddf, png_dir, nc_dir, config)
