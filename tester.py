import rioxarray
from utils import get_bbox_within_canada
from stacs import get_rcm_data, get_landcover_data

# Parameters
resolution_m = 30
tile_size = 224
rcm_filename = "./test_data/rcm_tile.tif"
landcover_filename = "./test_data/landcover_tile.tif"

# 1. Generate random bbox and degree resolution
bbox_info = get_bbox_within_canada(resolution_m, tile_size)
bbox = bbox_info["bbox"]
deg_res = bbox_info["deg_resolution"]

print("Generated BBox:", bbox)
print("Resolution in degrees (lon, lat):", deg_res)

# 2. Fetch RCM data
rcm_tile = get_rcm_data(bbox, deg_res)
if rcm_tile is not None:
    rcm_tile_squeezed = rcm_tile.squeeze(dim="time")
    rcm_tile_squeezed.rio.to_raster(rcm_filename)
    print(f"Saved RCM tile to {rcm_filename}")
else:
    print("RCM data not available, skipping save.")

# 3. Fetch Landcover data
landcover_tile = get_landcover_data(bbox, deg_res)
if landcover_tile is not None:
    landcover_tile_squeezed = landcover_tile.squeeze(dim="band")
    landcover_tile_squeezed.rio.to_raster(landcover_filename)
    print(f"Saved Landcover tile to {landcover_filename}")
else:
    print("Landcover data not available, skipping save.")
