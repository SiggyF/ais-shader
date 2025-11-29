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

    # Aggregate using lines
    line_width = config["visualization"]["line_width"]
    try:
        # If line_width > 0, we get AA (coverage). If 0, we get counts (if we specify count).
        # To ensure we get counts (or accumulated density), we should specify aggregation.
        # However, AA lines in Datashader 0.14+ might behave differently.
        # For now, let's use default for AA (which seems to be coverage 0-1) 
        # and explicit count for aliased.
        if line_width == 0:
            agg = cvs.line(gdf_local, geometry='geometry', agg=ds.count())
        else:
            agg = cvs.line(gdf_local, geometry='geometry', line_width=line_width)
            
    except ValueError as e:
        logger.warning(f"Rendering failed for tile {tile}: {e}")
        return
    
    # Check if empty (double check after rendering)
    agg_sum = agg.sum().item()
    agg_max = agg.max().item()
    if agg_sum == 0:
        logger.info(f"Tile {tile} is empty (after render). Skipping.")
        return
    
    logger.info(f"Tile {tile} stats: sum={agg_sum:.2f}, max={agg_max:.2f}")
        
    # Shade
    cmap = config["style"]["colormap"]
    img = tf.shade(agg, cmap=cmap, how='log', min_alpha=0)
    
    # Save PNG
    tile_png_dir = png_dir / str(tile.z) / str(tile.x)
    tile_png_dir.mkdir(parents=True, exist_ok=True)
    export_image(img, str(tile.y), export_path=str(tile_png_dir), background=None)
    
    # Save COG
    tile_name = f"tile_{tile.z}_{tile.x}_{tile.y}"
    tif_path = tiff_dir / f"{tile_name}.tif"
    cog_path = tiff_dir / f"{tile_name}_cog.tif"
    
    # Set geospatial info
    agg.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    agg.rio.write_crs("EPSG:3857", inplace=True)
    
    # Write temp GeoTIFF
    agg.rio.to_raster(tif_path)
    
    # Convert to COG
    dst_profile = cog_profiles.get("deflate")
    cog_translate(
        str(tif_path),
        str(cog_path),
        dst_profile,
        in_memory=True,
        quiet=True
    )
    
    # Clean up
    tif_path.unlink()
    
    logger.info(f"Saved PNG and COG for tile {tile}")


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
