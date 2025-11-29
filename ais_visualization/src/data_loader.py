import logging
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd
from pathlib import Path

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
    Load AIS data, convert to geometry, reproject, and extract coordinates.
    Returns a persisted Dask DataFrame with 'x' and 'y' columns.
    """
    logger.info(f"Loading data from {input_file}...")

    # Read as standard dask dataframe
    ddf = dd.read_parquet(input_file, engine="pyarrow")
    
    # Subset partitions if requested
    if partitions is not None:
        logger.info(f"Using first {partitions} partitions...")
        ddf = ddf.partitions[:partitions]

    logger.info("Data loaded (lazy). Converting WKB to Geometry...")

    # Construct meta
    meta_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries([], dtype="object"), crs="EPSG:4269"
    )

    # Map partitions
    ddf_geo = ddf.map_partitions(convert_to_gdf, meta=meta_gdf)

    # Convert to dask_geopandas object
    ddf_geo = dask_geopandas.from_dask_dataframe(ddf_geo, geometry="geometry")

    # Force CRS if needed
    if not hasattr(ddf_geo, "crs") or ddf_geo.crs is None:
        logger.info("Setting CRS manually to EPSG:4269...")
        ddf_geo.crs = "EPSG:4269"

    logger.info(f"Current CRS: {ddf_geo.crs}")

    # Reproject to Web Mercator (EPSG:3857)
    if ddf_geo.crs != "EPSG:3857":
        logger.info("Reprojecting to EPSG:3857...")
        ddf_geo = ddf_geo.to_crs("EPSG:3857")

    # Extract coordinates to standard dask dataframe (x, y)
    logger.info("Extracting coordinates for visualization...")
    
    meta_coords = pd.DataFrame({'x': [0.0], 'y': [0.0]})
    coords_ddf = ddf_geo.map_partitions(get_coords, meta=meta_coords)
    
    # Persist coordinates
    logger.info("Persisting coordinates in memory...")
    coords_ddf = coords_ddf.persist()
    
    return coords_ddf
