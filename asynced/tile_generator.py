import duckdb
import geopandas as gpd
import json
import os
import subprocess

DB_PATH = "./data/outputs/rcm_ard.duckdb"
OUT_DIR = "./data/outputs/tiles"
os.makedirs(OUT_DIR, exist_ok=True)

# 1. Export bboxes to GeoJSON
con = duckdb.connect(DB_PATH)
rows = con.execute("""
    SELECT id, bbox, province, census_div, census_subdiv
    FROM canada_bboxes
""").fetchall()

features = []
for row in rows:
    _id, bbox, province, cd, csd = row
    if not bbox:
        continue
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bbox[0], bbox[1]],
                [bbox[0], bbox[3]],
                [bbox[2], bbox[3]],
                [bbox[2], bbox[1]],
                [bbox[0], bbox[1]],
            ]]
        },
        "properties": {"id": _id, "province": province, "census_div": cd,
                       "census_subdiv": csd}
    })

bboxes_path = "./data/outputs/bboxes.geojson"
with open(bboxes_path, "w") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f)

print(f"✅ Exported {len(features)} bboxes")

# 2. Export shapefiles → GeoJSON
layers = {
    "census_div": "./data/inputs/census_div",
    "census_subdiv": "./data/inputs/census_subdiv",
    "prov_terr": "./data/inputs/prov_terr"
}
geojson_paths = {}
for lname, folder in layers.items():
    gdf = gpd.read_file(folder)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    gdf = gdf.explode(index_parts=False)
    gdf["layer"] = lname
    out_path = f"./data/outputs/{lname}.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    geojson_paths[lname] = out_path
    print(f"✅ Exported {lname} → {out_path}")

# 3. Tippecanoe: generate tiles
zoom_settings = {
    "prov_terr": ["-Z", "0", "-z", "6"],
    "census_div": ["-Z", "0", "-z", "8"],
    "census_subdiv": ["-Z", "0", "-z", "10"],
    "bboxes": ["-zg"],
}

for lname, path in {"bboxes": bboxes_path, **geojson_paths}.items():
    zoom_args = zoom_settings.get(lname, ["-zg"])
    cmd = [
        "tippecanoe",
        "-o", f"{OUT_DIR}/{lname}.mbtiles",
        "-l", lname,
        *zoom_args,
        "--drop-densest-as-needed",
        path
    ]
    subprocess.run(cmd, check=True)

    # Convert MBTiles → tile directory
    subprocess.run([
        "tile-join",
        "-e", f"{OUT_DIR}/{lname}_tiles",
        f"{OUT_DIR}/{lname}.mbtiles"
    ], check=True)

    print(f"🎉 Generated {lname} vector tiles at {OUT_DIR}/{lname}_tiles")
