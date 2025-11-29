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
    try:
        gdf_local = subset.compute()
    except Exception as e:
        logger.warning(f"Failed to compute subset for {tile}: {e}")
        return

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
        # Convert to DataArray with 'band' dimension
        # agg is a Dataset where each variable is a category
        # We want to stack them into a single DataArray with a 'band' dimension
        # But rioxarray expects 2D or 3D (band, y, x).
        # Let's convert the Dataset to a DataArray
        da = agg.to_array(dim="band")
        # Ensure fill value is 0
        da = da.fillna(0).astype("uint32")
    else:
        # Single band
        da = agg.fillna(0).astype("uint32")
        da = da.expand_dims(dim={'band': 1})

    da.rio.write_crs("EPSG:3857", inplace=True)
    da.rio.write_transform(transform, inplace=True)
    
    tiff_path = tiff_dir / f"tile_{tile.z}_{tile.x}_{tile.y}_counts.tif"
    da.rio.to_raster(tiff_path, tiled=True, compress="DEFLATE")
    
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

    # --- Save PNG (Visualization) ---
    # For PNG, we want a single image. If categorical, we can colorize by category.
    # But user wants "Electric Blue" palette.
    # If categorical, we should probably sum them up for the visual map?
    # Or use the categorical colormap?
    # The config has a single colormap list.
    # Let's sum them up for the PNG to keep the "Electric Blue" density map look.
    
    if isinstance(agg, xr.Dataset):
        # Sum all categories to get total density for visualization
        agg_visual = agg.to_array().sum(dim="variable")
    else:
        agg_visual = agg

    # Shade
    img = tf.shade(agg_visual, cmap=config["style"]["colormap"], min_alpha=0)
    
    # Save PNG
    png_path = png_dir / str(tile.z) / str(tile.x) / f"{tile.y}.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Export image
    with open(png_path, "wb") as f:
        f.write(img.to_bytes())
        
    logger.info(f"Saved PNG and TIFF for tile {tile}")


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
    logger.info(f"Found {len(tiles)} tiles to render.")
    
    # Create subdirectories
    png_dir = output_dir / "png"
    tiff_dir = output_dir / "tiff"
    tiff_dir.mkdir(parents=True, exist_ok=True)
    
    for tile in tiles:
        logger.info(f"Processing Tile: {tile}...")
        process_single_tile(tile, coords_ddf, png_dir, tiff_dir, config)
