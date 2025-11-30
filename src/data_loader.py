import logging
from pathlib import Path

import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

def convert_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a pandas DataFrame with a 'Shape' column (WKB bytes)
    to a GeoDataFrame.
    """
    if "Shape" not in df.columns:
        return df

    # Convert WKB to geometry
    gs = gpd.GeoSeries.from_wkb(df["Shape"])
    
    # We only need the geometry for visualization
    gdf = gpd.GeoDataFrame(geometry=gs, crs="EPSG:4269")
    return gdf

def get_coords(df):
    """Extract x, y coordinates from geometry."""
    if hasattr(df.geometry, 'get_coordinates'):
            return df.geometry.get_coordinates()
    else:
            return pd.DataFrame({'x': [], 'y': []})

def load_and_process_data(input_file: Path, partitions: int = None):
    """
    Load preprocessed AIS data (already has geometry and spatial partitions).
    """
    logger.info(f"Loading preprocessed data from {input_file}...")

    # Read dask_geopandas object directly
    ddf_geo = dask_geopandas.read_parquet(input_file)
    
    # Subset partitions if requested
    if partitions is not None:
        logger.info(f"Using first {partitions} partitions...")
        # Slicing drops spatial_partitions, so we need to preserve them
        original_spatial_partitions = ddf_geo.spatial_partitions
        ddf_geo = ddf_geo.partitions[:partitions]
        if original_spatial_partitions is not None:
             ddf_geo.spatial_partitions = original_spatial_partitions[:partitions]

    # Check for spatial partitions
    if ddf_geo.spatial_partitions is None:
        logger.warning("Spatial partitions not found in metadata. Calculating on the fly (this may take a while)...")
        # ddf_geo = ddf_geo.persist() # Ensure data is in memory
        ddf_geo.calculate_spatial_partitions()
        
        if ddf_geo.spatial_partitions is None:
             logger.error("Failed to calculate spatial partitions!")
             # We can still proceed but rendering might fail or be inefficient
        else:
             logger.info(f"Calculated {len(ddf_geo.spatial_partitions)} spatial partitions.")

    # Ensure CRS is correct (should be EPSG:3857 from preprocessing)
    if ddf_geo.crs != "EPSG:3857":
        logger.warning(f"Unexpected CRS: {ddf_geo.crs}. Expected EPSG:3857.")
    
    # Persist
    # logger.info("Persisting GeoDataFrame in memory...")
    # ddf_geo = ddf_geo.persist()
    
    return ddf_geo
