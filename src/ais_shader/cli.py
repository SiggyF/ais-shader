import logging
import sys
from pathlib import Path
import click
import tomllib

# Import from src modules
from .renderer import run_rendering
from .postprocessing import run_post_processing
from .preprocessing import run_preprocessing

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """
    AIS Visualization Pipeline CLI.
    """
    pass

@cli.command()
@click.option(
    "--config-file",
    type=click.Path(exists=True, path_type=Path),
    default=Path("config.toml"),
    help="Path to the configuration file.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("rendered"),
    help="Base directory for output.",
)
@click.option(
    "--scheduler",
    type=str,
    default=None,
    help="Address of the Dask scheduler (e.g., tcp://127.0.0.1:8786). If None, starts a local cluster.",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to input Parquet file (overrides config.toml).",
)
@click.option(
    "--resume-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to a run directory to resume.",
)
@click.option(
    "--bbox",
    nargs=4,
    type=float,
    default=None,
    help="Bounding box (minx miny maxx maxy) to override config.",
)
@click.option(
    "--zoom",
    type=int,
    default=None,
    help="Zoom level to override config.",
)
def render(config_file, output_dir, scheduler, input_file, resume_dir, bbox, zoom):
    """
    Render tiles from AIS data using Datashader.
    """
    run_rendering(config_file, output_dir, scheduler, input_file, resume_dir, bbox, zoom)

@cli.command()
@click.option("--run-dir", type=click.Path(exists=True, path_type=Path), required=True, help="Path to the run directory.")
@click.option("--base-zoom", type=int, default=7, help="Base zoom level to render.")
@click.option("--scheduler", type=str, default=None, help="Address of the Dask scheduler (e.g., tcp://127.0.0.1:8786).")
@click.option("--clean-intermediate", is_flag=True, help="Delete intermediate NetCDF files (Zoom 0 to base-zoom-1) after processing.")
@click.option("--cogs", is_flag=True, help="Export Cloud Optimized GeoTIFFs for the base zoom level.")
@click.option("--config-file", type=click.Path(exists=True, path_type=Path), default=Path("config.toml"), help="Path to the configuration file.")
def post_process(run_dir, base_zoom, scheduler, clean_intermediate, cogs, config_file):
    """
    Post-process Zarr tiles to PNGs and COGs.
    """
    run_post_processing(run_dir, base_zoom, scheduler, clean_intermediate, cogs, config_file)

@cli.command()
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
@click.option(
    "--scheduler",
    type=str,
    default=None,
    help="Address of the Dask scheduler (e.g., tcp://127.0.0.1:8786). If None, starts a local cluster.",
)
def preprocess(input_file, output_file, partitions, scheduler):
    """
    Preprocess AIS data (WKB -> GeoParquet -> Reproject).
    """
    run_preprocessing(input_file, output_file, partitions, scheduler)

if __name__ == "__main__":
    cli()
