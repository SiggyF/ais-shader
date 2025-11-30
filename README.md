# AIS Vessel Tracks Visualization

A scalable Python pipeline to visualize AIS vessel tracks from large Parquet datasets (e.g., 11GB) as tiled, high-quality maps.

## Features

- **Scalable Processing**: Built with [Dask](https://dask.org/) and [Datashader](https://datashader.org/) to handle datasets larger than memory.
- **Seamless Tiling**: Calculates a global maximum across all tiles to ensure consistent color scaling and eliminate edge artifacts.
- **Smart Transparency**: Custom "Electric Blue" colormap with gradual alpha transparency for low-density areas.
- **Full Pyramid**: Generates Zoom levels 0-7, allowing for smooth zooming from global view to details.
- **Robust Format**: Uses NetCDF for intermediate storage to handle multi-dimensional categorical data.
- **Dual Formats**: Exports both **PNG** (for display) and **Cloud Optimized GeoTIFF (COG)** (for analysis).
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

### 3. Rendering (Phase 1: Raw Data)

Generate raw count data (NetCDF) for the highest zoom level (e.g., Zoom 7). NetCDF is used to support multi-dimensional categorical data.

```bash
# Use input file from config.toml
uv run shade_tracks.py

# Override input file via CLI
uv run shade_tracks.py --input-file /path/to/other_dataset.parquet

# Use a shared Dask scheduler (recommended for large datasets)
uv run shade_tracks.py --scheduler tcp://127.0.0.1:8786
```

This will output `.nc` files to `rendered/run_YYYYMMDD_HHMMSS/tiff/`.

### 4. Post-Processing (Phase 2: Visualization)

Process the raw NetCDF files to generate seamless, transparent PNGs and lower zoom levels (pyramid).

```bash
# Run post-processing on a specific run directory
uv run post_process.py --run-dir rendered/run_YYYYMMDD_HHMMSS --base-zoom 7

# Optional: Clean up intermediate NetCDF files to save space
uv run post_process.py --run-dir rendered/run_YYYYMMDD_HHMMSS --base-zoom 7 --clean-intermediate
```

This script will:
1.  Calculate the **Global Max** density across all tiles to ensure consistent coloring (no seams).
2.  Render **PNGs** using a custom "Electric Blue" colormap with transparency for low counts.
3.  Generate **Pyramid** levels (Zoom 0-6) by aggregating the base zoom data.

## Pipeline Overview

1.  **Data Loading**: Reads the preprocessed GeoParquet file using `dask-geopandas`.
2.  **Tiling**: Calculates the list of Web Mercator tiles for the configured BBox and Zoom.
3.  **Processing**: For each tile:
    - Filters the dataset using spatial indexing (`.cx`).
    - Computes the subset to a local GeoDataFrame.
    - Renders the tracks using Datashader (`cvs.line`).
    - Applies the colormap and transparency.
    - Applies the colormap and transparency.
### Output Structure

The pipeline generates the following directory structure:

```
rendered/
  run_YYYYMMDD_HHMMSS/
    metadata.json       # Run configuration and details
    nc/                 # Intermediate NetCDF files (compressed)
      tile_7_*.nc       # Base zoom tiles
      tile_6_*.nc       # aggregated tiles
      ...
    png/                # Visualized PNG tiles
      7/                # Zoom 7
      6/                # Zoom 6
      ...
    tiff/               # Cloud Optimized GeoTIFFs (if --cogs used)
      tile_7_*.tif
```

### Storage Estimates

With `zlib` compression (level 5) and `int32` data types:
*   **Zoom 7**: ~93 MB (143 tiles)
*   **Zoom 10 (Estimated)**: ~6 GB (assuming ~9000 tiles)

### Stability Note

For the pyramid generation step (`post_process.py`), it is recommended to limit concurrency to avoid NetCDF/HDF5 locking issues:
```bash
# Run with a single worker for maximum stability
HDF5_USE_FILE_LOCKING=FALSE uv run dask worker tcp://127.0.0.1:8786 --nworkers 1 --memory-limit 8GB
```

## Project Structure

- `src/`: Source code modules.
    - `data_loader.py`: Data loading logic.
    - `renderer.py`: Rendering and export logic.
- `visualize_tracks.py`: Main entry point.
- `preprocess.py`: Data preprocessing script.
- `config.toml`: Configuration file.
