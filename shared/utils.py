import random
import math
import rasterio
import geopandas as gpd
from shapely.geometry import Point


CANADA_BBOX = [-141.002, 41.676, -52.63, 83.136]  # lon/lat bounds


def get_bbox_from_point(lon: float, lat: float, resolution_m: int, tile_size: int):
    """
    Generate a bbox centered at the given lon/lat.

    Returns:
        dict with 'bbox' (minlon, minlat, maxlon, maxlat) and 'deg_resolution' (deg_per_pixel_lon, deg_per_pixel_lat)
    """
    # meters per degree
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * math.cos(math.radians(lat))

    # convert resolution to degrees
    deg_per_pixel_lat = resolution_m / meters_per_deg_lat
    deg_per_pixel_lon = resolution_m / meters_per_deg_lon

    # half-extent = (tile_size-1)/2 pixels
    half_lat = ((tile_size - 1) / 2) * deg_per_pixel_lat
    half_lon = ((tile_size - 1) / 2) * deg_per_pixel_lon

    # bbox in lon/lat
    minlon = lon - half_lon
    maxlon = lon + half_lon
    minlat = lat - half_lat
    maxlat = lat + half_lat

    return {
        "bbox": (minlon, minlat, maxlon, maxlat),
        "deg_resolution": (deg_per_pixel_lon, deg_per_pixel_lat)
    }


def get_random_lon_lat_within_canada():
    """
    Returns a random (lon, lat) within Canada bounds.
    """
    lon = random.uniform(CANADA_BBOX[0], CANADA_BBOX[2])
    lat = random.uniform(CANADA_BBOX[1], CANADA_BBOX[3])
    return lon, lat


# Helper: save raster + band names
def save_with_bandnames(arr, filename, band_names=None):
    arr.rio.to_raster(filename)
    if band_names is not None:
        with rasterio.open(filename, "r+") as dst:
            for i, name in enumerate(band_names, start=1):
                dst.set_band_description(i, str(name))


def sample_points_per_geometry(shapefile, id_column, n_points_per_geom=1, seed=None):
    """
    Sample random lon/lat points from each geometry and attach the corresponding ID.

    Parameters:
        shapefile: path to geometry shapefile
        id_column: column name to get ID from
        n_points_per_geom: int, number of points to sample per geometry
        seed: int, optional random seed for reproducibility

    Returns:
        List of dicts with keys: lon, lat, ID
    """
    if seed is not None:
        random.seed(seed)

    # Load census subdivision shapefile
    gdf_cs = gpd.read_file(shapefile).to_crs(epsg=4326)

    results = []

    for idx, row in gdf_cs.iterrows():
        geom = row.geometry
        row_id = row.get(id_column, None)

        if geom.is_empty or row_id is None:
            continue

        minx, miny, maxx, maxy = geom.bounds

        for _ in range(n_points_per_geom):
            for attempt in range(1000):
                lon = random.uniform(minx, maxx)
                lat = random.uniform(miny, maxy)
                point = Point(lon, lat)
                if geom.contains(point):
                    results.append({
                        "lon": lon,
                        "lat": lat,
                        "ID": row_id
                    })
                    break
            else:
                print(f"Warning: Could not sample point inside geometry {row_id}")

    return results
