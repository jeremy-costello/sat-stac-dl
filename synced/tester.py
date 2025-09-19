import rioxarray
from shared.utils import get_bbox_from_point, get_random_lon_lat_within_canada, save_with_bandnames
from synced.stacs import get_rcm_data, get_landcover_data, get_hls_data
from synced.metadata import features_from_bbox


# Parameters
resolution_m = 30
tile_size = 256
provinces_shapefile = "./data/inputs/provinces_digital"

rcm_filename = "./data/outputs/rcm_tile.tif"
landcover_filename = "./data/outputs/landcover_tile.tif"
hls_filename = "./data/outputs/hls_tile.tif"


# 1. Generate random bbox and degree resolution
lon, lat = get_random_lon_lat_within_canada()
bbox_info = get_bbox_from_point(
    lon=lon,
    lat=lat,
    resolution_m=resolution_m,
    tile_size=tile_size
)
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
    band_names = list(landcover_tile.band.values)
    save_with_bandnames(landcover_tile, landcover_filename, band_names)
    print(f"Saved Landcover tile to {landcover_filename}")
else:
    print("Landcover data not available, skipping save.")

# 4. Fetch Sentinel HLS data (default collection = hls2-s30)
hls_tile = get_hls_data(bbox, deg_res, collection="hls2-s30")
if hls_tile is not None:
    hls_tile_squeezed = hls_tile.squeeze(dim="time")
    band_names = list(hls_tile_squeezed.band.values)
    save_with_bandnames(hls_tile_squeezed, hls_filename, band_names)
    print(f"Saved HLS Sentinel tile to {hls_filename}")
else:
    print("HLS data not available, skipping save.")

# 5. Get metadata
shapefiles = [
    {"path": "./data/inputs/prov_terr", "columns": ["PRNAME", "PRUID"]},
    {"path": "./data/inputs/census_div", "columns": ["CDNAME", "CDUID"]},
    {"path": "./data/inputs/census_subdiv", "columns": ["CSDNAME", "CSDUID"]},
]

for sf in shapefiles:
    results = features_from_bbox(
        bbox=bbox,
        shapefile_path=sf["path"],
        columns=sf["columns"],
        find_nearest=True  # returns nearest if no intersection
    )
    print(f"\nResults for {sf['path']}:")
    if results:
        for r in results:
            print(r)
    else:
        print("No features found.")