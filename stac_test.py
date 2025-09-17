import random
import rioxarray
import stackstac
from pystac_client import Client
from utils import get_bbox_within_canada

resolution_m = 30
tile_size = 224

# connect to the EODMS STAC API
stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
catalog = Client.open(stac_url)

# get bbox + degree resolution
bbox_info = get_bbox_within_canada(
    resolution_m=resolution_m,
    tile_size=tile_size
)
bbox = bbox_info["bbox"]
deg_res = bbox_info["deg_resolution"]

print("BBox:", bbox)
print("Resolution in degrees (lon, lat):", deg_res)

# search RCM ARD collection with bbox
search = catalog.search(
    collections=["rcm-ard"],
    bbox=bbox,
    datetime="2019-06-12/2048-01-01",
    limit=1,
    method="GET"
)
items = list(search.items())
if not items:
  print("Found no items!")
else:
  print(f"Found {len(items)} items")

  assets = [
    "rl",
    "rr",
    "data_mask",
    "local_inc_angle",
    "gamma_to_sigma_ratio",
    "local_contributing_area"
  ]

  # load to DataArray using degree resolution
  arr = stackstac.stack(
    items,
    assets=assets,
    epsg=4326,
    resolution=deg_res,   # <- degrees per pixel
    bounds_latlon=bbox
  )
  print(arr.shape)  # should be (time, assets, 224, 224)

  t_idx = random.randrange(len(arr['time']))

  # slice the array to that one time
  arr_single = arr.isel(time=[t_idx])

  print(arr_single.shape)  # (1, assets, 224, 224)

