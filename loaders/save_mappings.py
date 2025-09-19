import geopandas as gpd
import json

# File paths
prov_shp = "./data/inputs/prov_terr"
cd_shp = "./data/inputs/census_div"
csd_shp = "./data/inputs/census_subdiv"

# Load shapefiles
gdf_prov = gpd.read_file(prov_shp)
gdf_cd = gpd.read_file(cd_shp)
gdf_csd = gpd.read_file(csd_shp)

# 1) Province ID -> Name
pr_mapping = {str(row["PRUID"]): row["PRNAME"] for _, row in gdf_prov.iterrows()}
with open("./data/inputs/PR_mapping.json", "w", encoding="utf-8") as f:
    json.dump(pr_mapping, f, ensure_ascii=False, indent=2)

# 2) Census Division ID -> Name
cd_mapping = {str(row["CDUID"]): row["CDNAME"] for _, row in gdf_cd.iterrows()}
with open("./data/inputs/CD_mapping.json", "w", encoding="utf-8") as f:
    json.dump(cd_mapping, f, ensure_ascii=False, indent=2)

# 3) Census Subdivision ID -> Name
csd_mapping = {str(row["CSDUID"]): row["CSDNAME"] for _, row in gdf_csd.iterrows()}
with open("./data/inputs/CSD_mapping.json", "w", encoding="utf-8") as f:
    json.dump(csd_mapping, f, ensure_ascii=False, indent=2)

print("Mappings saved to ./data/inputs/PR_mapping.json, CD_mapping.json, CSD_mapping.json")
