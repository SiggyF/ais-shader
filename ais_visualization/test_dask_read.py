
import dask_geopandas
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_read():
    path = "/Users/baart_f/data/ais/AISVesselTracks2023_raw.parquet"
    logger.info(f"Testing read on {path}...")
    
    try:
        logger.info("Attempt 1: Default read_parquet")
        ddf = dask_geopandas.read_parquet(path)
        logger.info("Success 1!")
        print(ddf.head())
    except Exception as e:
        logger.error(f"Fail 1: {e}")

    try:
        logger.info("Attempt 2: gather_spatial_partitions=False")
        ddf = dask_geopandas.read_parquet(path, gather_spatial_partitions=False)
        logger.info("Success 2!")
        print(ddf.head())
    except Exception as e:
        logger.error(f"Fail 2: {e}")

    try:
        logger.info("Attempt 3: chunksize=100MB")
        ddf = dask_geopandas.read_parquet(path, chunksize="100MB")
        logger.info("Success 3!")
        print(ddf.head())
    except Exception as e:
        logger.error(f"Fail 3: {e}")

if __name__ == "__main__":
    test_read()
