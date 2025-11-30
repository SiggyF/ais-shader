# Release v0.3.0

## Highlights
- **Enhanced Documentation**: Comprehensive analysis of `line_width` settings, including a new [Line Width Analysis](docs/linewidth_analysis.md) document.
- **Visual Improvements**: Updated `README.md` with high-resolution screenshots and corrected colormap gradient bars.
- **Bug Fixes**: Fixed transparency issues for zero-count pixels and corrected pyramid normalization logic.

## Changes
- **Documentation**:
    - Added `docs/linewidth_analysis.md` detailing the trade-offs between aliased (`line_width=0`) and anti-aliased (`line_width=1`) rendering.
    - Updated `README.md` with new "Recommendation" section and "Advanced: Fair Density" discussion.
    - Added new screenshots (`overview.png`, `map_detail_*.png`) and colormap bars (`colormap_*.png`).
- **Code**:
    - `postprocessing.py`: Implemented per-level normalization and robust max calculation.
    - `config.toml`: Updated default colormap to `oslo`.
- **Tests**:
    - Added verification scripts in `tests/` to validate Datashader rendering behavior.

## Usage
To use the new "Fair Density" recommendation:
```bash
# For visualization (default)
ais-shader render ... --line-width 1

# For analysis (integer counts)
ais-shader render ... --line-width 0
```
