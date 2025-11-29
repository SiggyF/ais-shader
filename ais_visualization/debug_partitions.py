
import dask_geopandas
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug():
    path = "/Users/baart_f/data/ais/AISVesselTracks2023_processed.parquet"
    logger.info(f"Loading {path}...")
    ddf = dask_geopandas.read_parquet(path)
    
    logger.info(f"CRS: {ddf.crs}")
    
    if ddf.spatial_partitions is None:
        logger.info("Spatial partitions: None. Calculating...")
        ddf = ddf.persist()
        ddf.calculate_spatial_partitions()
    else:
        logger.info(f"Spatial partitions: Present ({len(ddf.spatial_partitions)})")
        
    if ddf.spatial_partitions is not None:
        bounds = ddf.spatial_partitions.total_bounds
        logger.info(f"Total Bounds (EPSG:3857): {bounds}")
        
        # Check intersection with US BBox
        # US BBox (EPSG:4326): -125, 24, -66, 49
        # In EPSG:3857 (approx):
        # -13914936, 2753444, -7347653, 6274861
        
        us_minx, us_miny, us_maxx, us_maxy = -13914936, 2753444, -7347653, 6274861
        logger.info(f"US BBox (Approx 3857): {us_minx}, {us_miny}, {us_maxx}, {us_maxy}")
        
        intersects = (
            (bounds[0] < us_maxx) & (bounds[2] > us_minx) &
            (bounds[1] < us_maxy) & (bounds[3] > us_miny)
        )
        logger.info(f"Intersects US? {intersects}")
    else:
        logger.error("Failed to calculate partitions.")

if __name__ == "__main__":
    debug()
