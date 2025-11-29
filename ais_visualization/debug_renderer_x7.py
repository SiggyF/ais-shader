
import dask_geopandas
import datashader as ds
import morecantile
import logging
import sys
from dask.distributed import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_render():
    client = Client()
    logger.info(f"Dashboard: {client.dashboard_link}")
    
    path = "/Users/baart_f/data/ais/AISVesselTracks2023_processed.parquet"
    logger.info(f"Loading {path}...")
    ddf = dask_geopandas.read_parquet(path)
    
    tile = morecantile.Tile(x=7, y=12, z=5)
    tms = morecantile.tms.get("WebMercatorQuad")
    bbox = tms.xy_bounds(tile)
    logger.info(f"Tile {tile} BBox: {bbox}")
    
    # Filter data like renderer does?
    # Renderer passes the whole ddf to cvs.line, Datashader handles filtering via x_range/y_range.
    # But we can optimize by filtering first to see if that changes anything.
    # Let's try passing the whole ddf first (or a subset to save time).
    
    # Actually, passing the whole 11GB ddf to cvs.line for one tile is inefficient but that's what the script does.
    # To be fast, I'll filter using cx first.
    # subset = ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
    # logger.info(f"Subset count: {len(subset)}")
    
    tile_size = 1024
    cvs = ds.Canvas(
        plot_width=tile_size, 
        plot_height=tile_size,
        x_range=(bbox.left, bbox.right),
        y_range=(bbox.bottom, bbox.top)
    )
    
    logger.info("Rendering...")
    if ddf.spatial_partitions is None:
        logger.error("Spatial partitions are None before rendering!")
    else:
        logger.info(f"Spatial partitions present (len={len(ddf.spatial_partitions)})")
        
    # Filter and compute to local GeoDataFrame
    # This avoids Datashader's internal dask issues and is efficient for single tiles
    subset = ddf.cx[bbox.left:bbox.right, bbox.bottom:bbox.top]
    logger.info("Computing subset...")
    gdf_local = subset.compute()
    logger.info(f"Local GeoDataFrame size: {len(gdf_local)}")
    
    # Pass local gdf
    agg = cvs.line(gdf_local, geometry='geometry', line_width=1)
    
    s = agg.sum().item()
    logger.info(f"Agg sum: {s}")
    
    if s == 0:
        logger.warning("Agg sum is 0!")
    else:
        logger.info("Agg sum is > 0. Rendering should work.")

if __name__ == "__main__":
    debug_render()
