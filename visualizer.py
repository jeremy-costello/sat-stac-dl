import duckdb
import folium
import json

# Connect and query - now including the file columns
conn = duckdb.connect('./outputs/metadata.duckdb')
results = conn.execute("SELECT bbox, landcover_file, rcm_file FROM raster_metadata").fetchall()

# Create map
m = folium.Map(location=[0, 0], zoom_start=2)

for row in results:
    bbox = row[0] # [min_lon, min_lat, max_lon, max_lat]
    landcover_file = row[1]
    rcm_file = row[2]
    
    # Determine color based on file availability
    if landcover_file is None:
        color = 'red'
        status = 'Missing landcover file'
    elif rcm_file is None:
        color = 'yellow'
        status = 'Missing RCM file'
    else:
        color = 'green'
        status = 'Both files available'
    
    # Create rectangle
    folium.Rectangle(
        bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
        popup=f"BBox: {bbox}<br>Status: {status}<br>Landcover: {landcover_file}<br>RCM: {rcm_file}",
        color=color,
        fill=True,
        fillOpacity=0.3,
        weight=2
    ).add_to(m)

# Add a legend - increased height to fit all content
legend_html = '''
<div style="position: fixed;
            bottom: 120px; left: 50px; width: 200px; height: 110px;
            background-color: white; border:2px solid grey; z-index:9999;
            font-size:14px; padding: 10px">
<b>Legend</b><br>
<i class="fa fa-square" style="color:red"></i> Missing landcover file<br>
<i class="fa fa-square" style="color:yellow"></i> Missing RCM file<br>
<i class="fa fa-square" style="color:green"></i> Both files available
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

m.save('./outputs/bbox_map.html')
