import rioxarray
import rasterio
from utils import get_bbox_within_canada, save_with_bandnames
from stacs import get_rcm_data, get_landcover_data, get_hls_data  # import new function


# Parameters
resolution_m = 30
tile_size = 256
rcm_filename = "./test_data/rcm_tile.tif"
landcover_filename = "./test_data/landcover_tile.tif"
hls_filename = "./test_data/hls_tile.tif"


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
    band_names = list(rcm_tile_squeezed.band.values)
    save_with_bandnames(rcm_tile_squeezed, rcm_filename, band_names)
    print(f"Saved RCM tile to {rcm_filename}")
else:
    print("RCM data not available, skipping save.")

# 3. Fetch Landcover data
landcover_tile = get_landcover_data(bbox, deg_res)
if landcover_tile is not None:
    band_names = list(landcover_tile.band.values)  # should be ["landcover-2010", "landcover-2015", "landcover-2020"]
    save_with_bandnames(landcover_tile, landcover_filename, band_names)
    print(f"Saved Landcover tile to {landcover_filename}")
else:
    print("Landcover data not available, skipping save.")

# 4. Fetch Sentinel HLS data (default collection = hls2-s30)
hls_tile = get_hls_data(bbox, deg_res, collection="hls2-s30")
if hls_tile is not None:
    hls_tile_squeezed = hls_tile.squeeze(dim="time")  # drop time dimension
    band_names = list(hls_tile_squeezed.band.values)  # should be ["B02", "B03", "B04"]
    save_with_bandnames(hls_tile_squeezed, hls_filename, band_names)
    print(f"Saved HLS Sentinel tile to {hls_filename}")
else:
    print("HLS data not available, skipping save.")
