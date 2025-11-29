# AIS Visualization Walkthrough

We have successfully built a scalable pipeline to visualize AIS vessel tracks for the entire US.

## Features
- **Scalable Processing**: Uses Dask and Datashader to handle the 11GB Parquet dataset.
- **Preprocessing**: Converts raw WKB data to a spatially partitioned Parquet file for efficient querying and rendering.
- **Line Rendering**: Renders tracks as `LineString` geometries using `cvs.line` with anti-aliasing (`line_width=1`).
- **Tiling**: Generates Web Mercator tiles (Zoom Level 5) using `morecantile`.
- **Modern Styling**: "Electric Blue" palette with transparency for low-density areas.
- **Dual Output**:
    - **PNG**: Organized in a TMS hierarchy (`png/z/x/y.png`) for web mapping.
    - **COG**: Cloud Optimized GeoTIFFs (`tiff/tile_z_x_y_cog.tif`) for GIS analysis.
- **Run Management**: Each execution creates a timestamped subdirectory in `rendered/`.

## Final Results (Refined Pipeline)

The pipeline has been refined to produce seamless, transparent, and multi-zoom visualizations.

### 1. GeoTIFF/NetCDF Generation (Zoom 7)
-   **Script**: `shade_tracks.py`
-   **Output**: NetCDF (`.nc`) files in `rendered/run_.../nc/`
-   **Format**: Raw counts, multi-band (categories aligned).

### 2. Post-Processing
-   **Script**: `post_process.py`
-   **Global Max**: Calculated across all Zoom 7 tiles (e.g., 256,511) to ensure consistent color scaling.
-   **Transparency**: "Electric Blue" colormap with gradual alpha for low-density areas.
-   **Pyramid**: Generated Zoom levels 0-6 by aggregating Zoom 7 data (summing counts).
-   **Output**: PNG tiles in `rendered/run_.../png/{z}/{x}/{y}.png`

### 3. Verification
-   **Structure**: Confirmed directory hierarchy `png/0/` to `png/7/`.
-   **Seams**: Global max scaling eliminates edge artifacts between tiles.
-   **Robustness**: Switched to NetCDF to handle multi-dimensional categorical data without errors.
-   **Parallelism**: Used Dask for efficient parallel rendering.

### Commands Used
```bash
# 1. Generate Raw Data
uv run shade_tracks.py --scheduler tcp://192.168.178.63:8786

# 2. Post-Process (Render & Pyramid)
uv run post_process.py --run-dir rendered/run_YYYYMMDD_HHMMSS --base-zoom 7 --scheduler tcp://192.168.178.63:8786
```

## Pipeline Steps

1.  **Preprocessing**:
    Converts raw data to a spatially partitioned GeoParquet file.
    ```bash
    uv run preprocess.py
    ```
    Output: `~/data/ais/AISVesselTracks2023_processed.parquet`

2.  **Visualization**:
    Renders tiles from the preprocessed data.
    ```bash
    uv run visualize_tracks.py
    ```
    Output: `rendered/run_YYYYMMDD_HHMMSS/`

## Code Structure
The code is modularized in `src/`:
- `src/data_loader.py`: Loads preprocessed data and preserves spatial partitions.
- `src/renderer.py`: Handles tiling, canvas aggregation (lines), styling, and export.
- `preprocess.py`: ETL script for spatial partitioning.
- `visualize_tracks.py`: Main CLI entry point.

## Output Structure
```
rendered/run_20251129_192452/
├── png/
│   └── 5/
│       ├── x/
│       │   └── y.png
└── tiff/
    ├── tile_5_x_y_cog.tif
```
