import xarray as xr
import numpy as np
import rioxarray
import os

# Create dummy data
data = np.random.randint(0, 100, size=(1, 100, 100, 2), dtype="uint32")
# Create dummy data with string coordinates
data = np.random.randint(0, 100, size=(1, 100, 100, 2), dtype="uint32")
coords = {
    "band": [1],
    "y": np.arange(100),
    "x": np.arange(100),
    "VesselGroup": ["Cargo", "Tanker"]
}
da = xr.DataArray(data, dims=("band", "y", "x", "VesselGroup"), coords=coords, name="counts")

# Add spatial ref
da.rio.write_crs("EPSG:3857", inplace=True)

# Save with compression
encoding = {"counts": {"zlib": True, "complevel": 5}}
da.to_netcdf("test_compressed_v2.nc", encoding=encoding, engine="netcdf4")

# Read back
ds = xr.open_dataset("test_compressed_v2.nc", engine="netcdf4")
print("Original Sum:", da.sum().values)
print("Saved Sum:", ds["counts"].sum().values)

if da.sum().values == ds["counts"].sum().values:
    print("SUCCESS: Data preserved.")
else:
    print("FAILURE: Data lost.")
