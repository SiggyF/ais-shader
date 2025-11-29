import logging
import sys
import time
from pathlib import Path
import click
import tomllib
from dask.distributed import Client

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
    "--partitions",
    type=int,
    default=None,
    help="Number of partitions to process (for testing).",
)
def main(config_file: Path, output_dir: Path, partitions: int):
    """
    Visualize AIS vessel tracks using Dask and Datashader.
    """
    # Load Config
    with open(config_file, "rb") as f:
        config = tomllib.load(f)
        
    logger.info(f"Loaded configuration from {config_file}")
    
    # Get input file from config (CLI override could be added but keeping it simple)
    input_file = Path(config["data"]["input_file"])

    logger.info("Starting Dask Distributed Client...")
    client = Client()
    logger.info(f"Dask Dashboard link: {client.dashboard_link}")

    # Create run directory with timestamp
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output will be saved to: {run_dir}")

    try:
        # Load Data
        coords_ddf = load_and_process_data(input_file, partitions)
        
        # Render Tiles
        render_tiles(coords_ddf, run_dir, config)
        
        logger.info("Done!")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
