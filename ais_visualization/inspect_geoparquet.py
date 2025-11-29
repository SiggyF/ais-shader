
import geopandas as gpd
import pyarrow.parquet as pq
import json
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def inspect_file(path):
    logger.info(f"Inspecting {path}...")
    try:
        # Check Parquet metadata
        table = pq.read_metadata(path)
        metadata = table.metadata
        logger.info(f"Parquet Metadata keys: {metadata.keys()}")
        
        if b'geo' in metadata:
            logger.info("Found 'geo' metadata!")
            geo_meta = json.loads(metadata[b'geo'])
            logger.info(json.dumps(geo_meta, indent=2))
        else:
            logger.info("No 'geo' metadata found.")
            
        # Try reading with geopandas
        logger.info("Attempting gpd.read_parquet...")
        gdf = gpd.read_parquet(path)
        logger.info(f"Success! CRS: {gdf.crs}")
        logger.info(gdf.head())
        
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    inspect_file("/Users/baart_f/data/ais/AISVesselTracks2023_processed.parquet")
