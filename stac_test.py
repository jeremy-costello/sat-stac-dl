import rioxarray
import stackstac
from pystac_client import Client

# connect to the EODMS STAC API
stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
catalog = Client.open(stac_url)

# define bbox: [west, south, east, north]
bbox = [-75.733480,45.412189,-75.673227,45.437249]

# search RCM ARD collection with bbox
search = catalog.search(
    collections=["rcm-ard"],
    bbox=bbox,
    datetime="2025-01-01/2025-09-15",  # optional temporal filter
    limit=5,
    method="GET"
)

items = list(search.items())
print(f"Found {len(items)} items")

# pick your desired asset key
asset_key = "local_contributing_area"

# load to DataArray
arr = stackstac.stack(
    items,
    assets=[asset_key],
    epsg=3857,  # 326**
    resolution=30,
    bounds_latlon=bbox
)

# Ensure correct CRS is set
arr = arr.rio.write_crs("EPSG:4326")

# Export to GeoTIFF
arr.squeeze().rio.to_raster("rcm_ard_rl.tif")
