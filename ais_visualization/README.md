# AIS Vessel Tracks Visualization

A scalable Python pipeline to visualize AIS vessel tracks from large Parquet datasets (e.g., 11GB) as tiled, high-quality maps.

## Features

- **Scalable Processing**: Built with [Dask](https://dask.org/) and [Datashader](https://datashader.org/) to handle datasets larger than memory.
- **Tiled Output**: Generates Web Mercator tiles (Zoom Level 5 by default) compatible with web maps (TMS).
- **Dual Formats**: Exports both **PNG** (for display) and **Cloud Optimized GeoTIFF (COG)** (for analysis).
- **Modern Styling**: "Electric Blue" colormap with transparency for low-density areas.
- **Anti-Aliasing**: Renders tracks as smooth lines (`LineString`) with anti-aliasing.
- **Configurable**: All settings (bbox, zoom, palette) are defined in `config.toml`.

## Troubleshooting Data

Some Marine Cadastre datasets (e.g., 2024) may have broken zip files. Fix them using:
```bash
zip -FF AISVesselTracks2024.zip --out AISVesselTracks2024-fixed.zip
```

## Installation

This project uses `uv` for dependency management.

```bash
# Install dependencies
uv sync
```

## Usage

### 1. Preprocessing (One-time)

Convert the raw AIS data (WKB Parquet or GPKG) into a spatially partitioned GeoParquet file. This significantly speeds up rendering.

**From Parquet:**
```bash
uv run preprocess.py --input-file /path/to/raw.parquet --output-file /path/to/processed.parquet
```

**From GPKG:**
```bash
uv run preprocess.py --input-file /path/to/data.gpkg --output-file /path/to/processed.parquet
```
*Note: GPKG conversion via Python can be slow. For faster results, use `ogr2ogr` first:*
```bash
ogr2ogr -f Parquet -t_srs EPSG:3857 raw.parquet input.gpkg
uv run preprocess.py --input-file raw.parquet --output-file processed.parquet
```

### 2. Configuration

Edit `config.toml` to customize the visualization:

```toml
[data]
input_file = "/path/to/processed.parquet"

[visualization]
zoom = 5
tile_size = 1024
# line_width = 1  # Anti-aliased lines (values are coverage 0-1)
line_width = 0    # Aliased lines (values are integer counts)
bbox = [-125.0, 24.0, -66.0, 49.0]  # US Bounds

[style]
colormap = ["#001133", "#0044aa", "#00aaff", "#00ffff", "#ffffff"]
```

**Note on GeoTIFF Values:**
- If `line_width = 1` (default for aesthetics), the output GeoTIFFs contain **anti-aliased coverage values** (typically 0.0 to 1.0 per pixel, or accumulated if overlapping).
- If `line_width = 0`, the output GeoTIFFs contain **raw integer counts** of vessel tracks passing through each pixel. Use this for analysis.

### 3. Rendering

Run the visualization pipeline:

```bash
uv run visualize_tracks.py
```

```bash
# Use input file from config.toml
uv run visualize_tracks.py

# Override input file via CLI
uv run visualize_tracks.py --input-file /path/to/other_dataset.parquet

# Use a shared Dask scheduler (recommended for large datasets)
uv run visualize_tracks.py --scheduler tcp://127.0.0.1:8786
```

The script will:
1.  Load the preprocessed data.
2.  Generate tiles for the specified bounding box and zoom level.
3.  Save output to `rendered/run_YYYYMMDD_HHMMSS/`.

## Pipeline Overview

1.  **Data Loading**: Reads the preprocessed GeoParquet file using `dask-geopandas`.
2.  **Tiling**: Calculates the list of Web Mercator tiles for the configured BBox and Zoom.
3.  **Processing**: For each tile:
    - Filters the dataset using spatial indexing (`.cx`).
    - Computes the subset to a local GeoDataFrame.
    - Renders the tracks using Datashader (`cvs.line`).
    - Applies the colormap and transparency.
    - Exports as PNG and COG.

## Project Structure

- `src/`: Source code modules.
    - `data_loader.py`: Data loading logic.
    - `renderer.py`: Rendering and export logic.
- `visualize_tracks.py`: Main entry point.
- `preprocess.py`: Data preprocessing script.
- `config.toml`: Configuration file.
