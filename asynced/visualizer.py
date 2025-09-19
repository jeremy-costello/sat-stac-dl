import duckdb
import folium
import os

# -----------------------------
# Config
# -----------------------------
DB_PATH = './data/outputs/rcm_ard_tiles.duckdb'
OUTPUT_HTML = './data/outputs/bbox_map.html'

# Connect to DB
con = duckdb.connect(DB_PATH)

# Query relevant fields
results = con.execute("""
    SELECT bbox, landcover_items, rcm_items, province, census_div, census_subdiv 
    FROM rcm_ard_tiles
""").fetchall()

# -----------------------------
# Initialize Folium map
# -----------------------------
m = folium.Map(location=[60, -95], zoom_start=4)  # roughly centered on Canada

# -----------------------------
# Add rectangles
# -----------------------------
for row in results:
    bbox, landcover_items, rcm_items, province, census_div, census_subdiv = row
    if not bbox:
        continue

    # Determine color based on item availability
    if not landcover_items:
        color = 'red'
        status = 'Missing landcover items'
    elif not rcm_items:
        color = 'yellow'
        status = 'Missing RCM items'
    else:
        color = 'green'
        status = 'Both items available'

    # Create rectangle with popup
    folium.Rectangle(
        bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],  # [min_lat, min_lon], [max_lat, max_lon]
        popup=f"<b>Province:</b> {province}<br>"
              f"<b>Census Division:</b> {census_div}<br>"
              f"<b>Census Subdivision:</b> {census_subdiv}<br>"
              f"<b>Status:</b> {status}",
        color=color,
        fill=True,
        fillOpacity=0.3,
        weight=2
    ).add_to(m)

# -----------------------------
# Add a legend
# -----------------------------
legend_html = '''
<div style="position: fixed;
            bottom: 120px; left: 50px; width: 200px; height: 110px;
            background-color: white; border:2px solid grey; z-index:9999;
            font-size:14px; padding: 10px">
<b>Legend</b><br>
<i class="fa fa-square" style="color:red"></i> Missing landcover items<br>
<i class="fa fa-square" style="color:yellow"></i> Missing RCM items<br>
<i class="fa fa-square" style="color:green"></i> Both items available
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

# -----------------------------
# Save map
# -----------------------------
os.makedirs(os.path.dirname(OUTPUT_HTML), exist_ok=True)
m.save(OUTPUT_HTML)
print(f"✅ Map saved to {OUTPUT_HTML}")
