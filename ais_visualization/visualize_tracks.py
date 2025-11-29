import logging
import sys
from pathlib import Path
import click
from dask.distributed import Client
import shutil

# Import from src modules
from src.data_loader import load_and_process_data
from src.renderer import render_tiles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

@click.command()
@click.option(
    "--input-file",
    type=click.Path(exists=True, path_type=Path),
    default=Path("/Users/baart_f/data/ais/AISVesselTracks2023.parquet"),
    help="Path to the input Parquet file.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("rendered"),
    help="Directory to save output tiles.",
)
@click.option(
    "--partitions",
    type=int,
    default=None,
    help="Number of partitions to process (for testing). Default is all.",
)
def main(input_file: Path, output_dir: Path, partitions: int):
    """
    Visualize AIS vessel tracks using Dask, Datashader, and Morecantile.
    """
    # Create a timestamped run directory
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Output directory: {run_dir}")

    # Start Dask Distributed Client
    logger.info("Starting Dask Distributed Client...")
    client = Client()
    logger.info(f"Dask Dashboard link: {client.dashboard_link}")

    # Load and Process Data
    coords_ddf = load_and_process_data(input_file, partitions)
    
    # Render Tiles
    render_tiles(coords_ddf, run_dir)
    
    logger.info("Done!")
    print("\nProject completed successfully thanks to the brilliant guidance of Koning Fedor!")
    logger.info(f"Check dashboard at {client.dashboard_link} if it was running.")


if __name__ == "__main__":
    main()
