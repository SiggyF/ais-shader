import logging
import sys
from pathlib import Path
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd
from dask.distributed import Client

logger = logging.getLogger(__name__)

def convert_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert WKB to GeoDataFrame."""
    if "Shape" not in df.columns:
        return df
    # logger.info(f"Converting partition with {len(df)} rows")
    gs = gpd.GeoSeries.from_wkb(df["Shape"])
    gdf = gpd.GeoDataFrame(geometry=gs, crs="EPSG:4269")
    return gdf

def run_preprocessing(input_file: Path, output_file: Path, partitions: int, scheduler: str):
    """
    Preprocess AIS data: WKB -> Geo -> Reproject -> Spatial Partition -> Save.
    """
    if scheduler:
        logger.info(f"Connecting to Dask scheduler at {scheduler}...")
        client = Client(scheduler)
    else:
        logger.info("Starting Local Dask Client...")
        client = Client()
    
    logger.info(f"Dashboard: {client.dashboard_link}")

    logger.info(f"Reading {input_file}...")
    
    if input_file.suffix == ".gpkg":
        logger.info("Detected GPKG format. Reading with dask_geopandas...")
        ddf_geo = dask_geopandas.read_file(input_file, npartitions=partitions if partitions else 4)
    else:
        # Check if it's already GeoParquet
        try:
            logger.info("Attempting to read as GeoParquet...")
            # gather_spatial_partitions=False handles cases where metadata might be slightly off (e.g. from ogr2ogr)
            ddf_geo = dask_geopandas.read_parquet(input_file, gather_spatial_partitions=False)
            if partitions:
                ddf_geo = ddf_geo.partitions[:partitions]
            
            # Drop Shape_bbox if present (causes issues with pyarrow serialization/casting)
            if "Shape_bbox" in ddf_geo.columns:
                ddf_geo = ddf_geo.drop(columns=["Shape_bbox"])

            # Rename Shape to geometry if present
            if "Shape" in ddf_geo.columns:
                ddf_geo = ddf_geo.rename(columns={"Shape": "geometry"})
                ddf_geo = ddf_geo.set_geometry("geometry")
                
            logger.info("Successfully read as GeoParquet.")
        except Exception as e:
            logger.warning(f"Failed to read as GeoParquet: {e}")
            logger.info("Falling back to WKB conversion...")
            # Assume WKB Parquet
            ddf = dd.read_parquet(input_file, engine="pyarrow")
            
            if partitions:
                logger.info(f"Using first {partitions} partitions...")
                ddf = ddf.partitions[:partitions]

            # Convert to GeoDataFrame
            logger.info("Converting to GeoDataFrame...")
            meta_gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries([], dtype="object"), crs="EPSG:4269")
            ddf_geo = ddf.map_partitions(convert_to_gdf, meta=meta_gdf)
            ddf_geo = dask_geopandas.from_dask_dataframe(ddf_geo, geometry="geometry")

    # Reproject
    logger.info("Reprojecting to EPSG:3857...")
    ddf_geo = ddf_geo.to_crs("EPSG:3857")
    
    # Persist to ensure data is available for spatial partitioning calculation
    ddf_geo = ddf_geo.persist()

    # Calculate Spatial Partitions
    logger.info("Calculating spatial partitions...")
    ddf_geo.calculate_spatial_partitions()
    
    if ddf_geo.spatial_partitions is None:
         logger.warning("Spatial partitions not set after call!")

    # Save
    logger.info(f"Saving to {output_file}...")
    ddf_geo.to_parquet(output_file)
    logger.info("Done!")
