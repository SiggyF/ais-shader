# Architecture & Design

## Overview
The AIS Visualization pipeline is designed to process massive datasets (10GB+) of vessel tracks and render them into high-resolution, tiled maps. It prioritizes scalability, memory efficiency, and visual quality.

## Technology Stack
- **Dask**: For parallel, out-of-core processing. It handles data loading, partitioning, and distributed computation.
- **Datashader**: For high-performance rasterization of vector data. It aggregates millions of points/lines into grids without overplotting issues.
- **Xarray & Zarr**: For efficient storage of multi-dimensional raster data. Zarr provides chunked, compressed storage ideal for cloud and parallel access.
- **Click**: For a robust, composable Command Line Interface (CLI).
- **GeoPandas & PyArrow**: For efficient spatial data handling and I/O.

## Architectural Considerations

### 1. Spatial Partitioning
Raw AIS data is often unsorted. To enable efficient rendering, we first preprocess the data into spatially partitioned GeoParquet files. This allows Dask to load only the relevant data chunks for each tile, significantly reducing memory usage and I/O.

### 2. The "Global Max" Problem
To create a seamless map where colors mean the same thing across all tiles, we must normalize pixel values against a **global maximum** density.
- **Phase 1 (Rendering)**: Each tile is rendered independently to a Zarr array (raw counts).
- **Phase 2 (Post-processing)**: We compute the global maximum across *all* tiles.
- **Phase 3 (Visualization)**: We re-process the tiles, applying the colormap normalized by this global max.

### 3. Memory Management
Processing high-zoom levels (e.g., Zoom 10) involves thousands of tiles.
- **Batching**: We process tiles in batches (e.g., 20) to control memory pressure.
- **Resource Monitoring**: A background thread monitors RAM usage and pauses submission if thresholds are exceeded.
- **Explicit GC**: We force garbage collection after batches to prevent memory leaks in long-running processes.

## Known Issues & Limitations
- **Zarr Serialization**: We explicitly disable compression for the `spatial_ref` coordinate to avoid `numpy.int64` serialization warnings in some versions of Xarray/Zarr.
- **GPKG Performance**: Reading from GeoPackage is significantly slower than Parquet. Always preprocess to Parquet first.
- **Edge Artifacts**: Without the global max normalization, tiles would have individual color scales, creating visible "checkerboard" artifacts at tile boundaries.

## Future Improvements
- **Dynamic Tiling**: Serve tiles dynamically from the raw data using a tile server (e.g., TiPpecanoe or a custom Python server) instead of pre-rendering everything.
- **Vector Tiles**: For lower zoom levels, vector tiles (MVT) might offer better interactivity than raster PNGs.
