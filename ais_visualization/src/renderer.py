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

def render_tiles(coords_ddf, output_dir: Path, zoom: int = 5):
    """
    Render tiles for the US using Datashader and save as PNG and COG.
    """
    # Define TileMatrixSet (WebMercatorQuad)
    tms = morecantile.tms.get("WebMercatorQuad")
    
    # Define US Bounding Box (approximate)
    # MinX, MinY, MaxX, MaxY
    # EPSG:4326: -125, 24, -66, 49
    us_bbox = (-125.0, 24.0, -66.0, 49.0)
    
    logger.info(f"Generating tiles for US BBox {us_bbox} at Zoom {zoom}...")
    
    tiles = list(tms.tiles(*us_bbox, zooms=[zoom]))
    logger.info(f"Found {len(tiles)} tiles to render.")
    
    # Create subdirectories for PNG (TMS) and TIFF
    png_dir = output_dir / "png"
    tiff_dir = output_dir / "tiff"
    tiff_dir.mkdir(parents=True, exist_ok=True)
    
    # Define Modern "Electric" Colormap with Transparency
    # We want the lowest values to be fully transparent.
    # We rely on min_alpha=0 in tf.shade to handle the fade out.
    electric_cmap = [
        "#001133",   # Dark Blue (will be transparent due to min_alpha)
        "#0044aa",   # Medium Blue
        "#00aaff",   # Cyan
        "#00ffff",   # Bright Cyan
        "#ffffff"    # White
    ]
    
    for tile in tiles:
        logger.info(f"Processing Tile: {tile}...")
        
        # Get bounds in EPSG:3857
        bbox = tms.xy_bounds(tile)
        
        # Define canvas for this tile
        tile_size = 1024
        # We use line rendering which supports Anti-Aliasing via line_width
        cvs = ds.Canvas(
            plot_width=tile_size, 
            plot_height=tile_size,
            x_range=(bbox.left, bbox.right),
            y_range=(bbox.bottom, bbox.top)
        )
        
        # Filter data for this tile using spatial index
        # This is more robust than passing the full dask ddf to datashader
        subset = coords_ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
        
        # Compute to local GeoDataFrame
        try:
            gdf_local = subset.compute()
        except Exception as e:
            logger.warning(f"Failed to compute subset for {tile}: {e}")
            continue
            
        if len(gdf_local) == 0:
            logger.info(f"Tile {tile} is empty. Skipping.")
            continue

        # Aggregate using lines
        # line_width=1 enables anti-aliasing in Datashader
        try:
            agg = cvs.line(gdf_local, geometry='geometry', line_width=1)
        except ValueError as e:
            logger.warning(f"Rendering failed for tile {tile}: {e}")
            continue
        
        # Check if empty
        if agg.sum() == 0:
            logger.info(f"Tile {tile} is empty. Skipping.")
            continue
            
        # Shade
        # how='log' reveals structure better across dynamic ranges
        # min_alpha=0 ensures the background is transparent
        img = tf.shade(agg, cmap=electric_cmap, how='log', min_alpha=0)
        
        # Prepare TMS directory structure for PNG: png/z/x/
        tile_png_dir = png_dir / str(tile.z) / str(tile.x)
        tile_png_dir.mkdir(parents=True, exist_ok=True)
        
        # Filenames
        # PNG: z/x/y.png
        png_path = tile_png_dir / f"{tile.y}.png"
        
        # TIFF: Flat structure in tiff/
        tile_name = f"tile_{tile.z}_{tile.x}_{tile.y}"
        tif_path = tiff_dir / f"{tile_name}.tif"
        cog_path = tiff_dir / f"{tile_name}_cog.tif"
        
        # Save PNG
        # export_image appends .png, so we provide path without extension? 
        # No, export_image takes a name and a path. 
        # export_image(img, name, export_path) -> export_path/name.png
        # To get exact control, we can use tf.save or just standard image saving if it's an xarray image.
        # Datashader's export_image is convenient but opinionated.
        # Let's use export_image but be careful with the name.
        export_image(img, str(tile.y), export_path=str(tile_png_dir), background=None)
        
        # Save COG
        # First set geospatial info on the agg xarray
        agg.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
        agg.rio.write_crs("EPSG:3857", inplace=True)
        
        # Write temp GeoTIFF
        agg.rio.to_raster(tif_path)
        
        # Convert to COG using rio-cogeo
        dst_profile = cog_profiles.get("deflate")
        cog_translate(
            str(tif_path),
            str(cog_path),
            dst_profile,
            in_memory=True,
            quiet=True
        )
        
        # Clean up temp tif
        tif_path.unlink()
        
        logger.info(f"Saved {png_path} and {cog_path}")
