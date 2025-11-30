
import dask_geopandas
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug():
    path = "/Users/baart_f/data/ais/AISVesselTracks2024_processed.parquet"
    logger.info(f"Loading {path}...")
    ddf = dask_geopandas.read_parquet(path)
    
    logger.info(f"Columns: {ddf.columns}")
    logger.info(f"VesselGroup dtype: {ddf['VesselGroup'].dtype}")
    
    # Check if it has known categories if it is categorical
    if hasattr(ddf['VesselGroup'].dtype, 'categories'):
         logger.info(f"Categories: {ddf['VesselGroup'].dtype.categories}")
    else:
         logger.info("Not a categorical dtype.")

    # Peek at values
    logger.info(f"Head: {ddf['VesselGroup'].head()}")

if __name__ == "__main__":
    debug()
