import logging
import sys
from pathlib import Path
import click
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd
from dask.distributed import Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def convert_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert WKB to GeoDataFrame."""
    if "Shape" not in df.columns:
        return df
    # logger.info(f"Converting partition with {len(df)} rows")
    gs = gpd.GeoSeries.from_wkb(df["Shape"])
    gdf = gpd.GeoDataFrame(geometry=gs, crs="EPSG:4269")
    return gdf

@click.command()
@click.option(
    "--input-file",
    type=click.Path(exists=True, path_type=Path),
    default=Path("/Users/baart_f/data/ais/AISVesselTracks2023.parquet"),
    help="Path to input Parquet file.",
)
@click.option(
    "--output-file",
    type=click.Path(path_type=Path),
    default=Path("/Users/baart_f/data/ais/AISVesselTracks2023_processed.parquet"),
    help="Path to output processed Parquet file.",
)
@click.option(
    "--partitions",
    type=int,
    default=None,
    help="Number of partitions to process (for testing).",
)
def main(input_file: Path, output_file: Path, partitions: int):
    """
    Preprocess AIS data: WKB -> Geo -> Reproject -> Spatial Partition -> Save.
    """
    logger.info("Starting Dask Client...")
    client = Client()
    logger.info(f"Dashboard: {client.dashboard_link}")

    logger.info(f"Reading {input_file}...")
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
    # It appears this method modifies in-place or returns None in this version?
    # Based on test_spatial.py, it returned None.
    # Let's try calling it without assignment, OR check if it modified the object.
    # Actually, dask objects are usually immutable. 
    # If it returns None, maybe it failed? 
    # But wait, if it's in-place, ddf_geo should be modified.
    ddf_geo.calculate_spatial_partitions()
    
    # However, dask dataframes are immutable. 
    # If it returns None, it's very strange for Dask.
    # Let's assume for a moment it IS in-place on the dask graph wrapper?
    # No, that's unlikely.
    
    # Let's try to see if ddf_geo has spatial_partitions after the call if we don't assign.
    if ddf_geo.spatial_partitions is None:
         logger.warning("Spatial partitions not set after call!")

    # Save
    logger.info(f"Saving to {output_file}...")
    ddf_geo.to_parquet(output_file)
    logger.info("Done!")

if __name__ == "__main__":
    main()
