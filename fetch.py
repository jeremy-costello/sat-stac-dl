import rioxarray
from utils import get_bbox_within_canada
from stacs import get_rcm_data, get_landcover_data
from metadata import features_from_bbox


SHAPEFILES = [
    {"path": "./inputs/prov_terr", "columns": ["PRNAME", "PRUID"], "key": "PR"},
    {"path": "./inputs/census_div", "columns": ["CDNAME", "CDUID"], "key": "CD"},
    {"path": "./inputs/census_subdiv", "columns": ["CSDNAME", "CSDUID"], "key": "CSD"},
]


def fetch_data(resolution_m=30, tile_size=256):
    """
    Fetch RCM data, and if available, also fetch Landcover and metadata.

    Returns:
        dict with bbox, deg_res, rcm, landcover, and metadata
    """
    # 1. Generate random bbox and resolution
    bbox_info = get_bbox_within_canada(resolution_m, tile_size)
    bbox = bbox_info["bbox"]
    deg_res = bbox_info["deg_resolution"]

    result = {
        "resolution_m": resolution_m,
        "tile_size": tile_size,
        "bbox": bbox,
        "deg_res": deg_res,
        "rcm": None,
        "landcover": None,
        "metadata": {}
    }

    print("Generated BBox:", bbox)
    print("Resolution in degrees (lon, lat):", deg_res)

    # 2. Fetch RCM data
    rcm_tile = get_rcm_data(bbox, deg_res)
    if rcm_tile is not None:
        result["rcm"] = rcm_tile
        print("Fetched RCM data.")
    else:
        print("RCM data not available.")

    # 3. Fetch Landcover data
    landcover_tile = get_landcover_data(bbox, deg_res)
    if landcover_tile is not None:
        result["landcover"] = landcover_tile
        print("Fetched Landcover data.")
    else:
        print("Landcover data not available.")

    # 4. Fetch metadata from shapefiles
    metadata = {}
    for sf in SHAPEFILES:
        results = features_from_bbox(
            bbox=bbox,
            shapefile_path=sf["path"],
            columns=sf["columns"],
            find_nearest=True
        )
        metadata[sf["key"]] = results
    result["metadata"] = metadata
    print("Fetched metadata.")

    return result


if __name__ == "__main__":
    data = fetch_data()
    print("\nFinal result dictionary:")
    for k, v in data.items():
        if isinstance(v, dict):
            print(f"{k}:")
            for subk, subv in v.items():
                print(f"  {subk}: {subv}")
        else:
            print(f"{k}: {type(v)}")
