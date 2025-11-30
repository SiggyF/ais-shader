import dask_geopandas
import logging
import sys
from pathlib import Path
import dask_geopandas
import logging
import sys
from pathlib import Path
import tomllib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_bbox(config_path):
    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        
        input_file = config["data"]["input_file"]
        logger.info(f"Reading {input_file}...")
        
        ddf = dask_geopandas.read_parquet(input_file)
        
        logger.info("Calculating total bounds...")
        # dask_geopandas.total_bounds computes the union of all partition bounds
        bounds = ddf.total_bounds.compute()
        
        logger.info(f"Total Bounds (EPSG:3857): {bounds}")
        
        # Convert to EPSG:4326
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        minx, miny, maxx, maxy = bounds
        lon_min, lat_min = transformer.transform(minx, miny)
        lon_max, lat_max = transformer.transform(maxx, maxy)
        
        print(f"BBOX (EPSG:4326): [{lon_min}, {lat_min}, {lon_max}, {lat_max}]")
        
    except Exception as e:
        logger.error(f"Failed to calculate bbox: {e}")
        sys.exit(1)

if __name__ == "__main__":
    calculate_bbox("config.toml")
