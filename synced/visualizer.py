import duckdb
import folium
import json

# Connect and query - now including the geographic info
conn = duckdb.connect('./data/outputs/rcm_ard_tiles.duckdb')
results = conn.execute("SELECT bbox, landcover_file, rcm_file, province, census_div, census_subdiv FROM raster_metadata").fetchall()

m = folium.Map(
    location=[0, 0], 
    zoom_start=2
)

for row in results:
    bbox = row[0] # [min_lon, min_lat, max_lon, max_lat]
    landcover_file = row[1]
    rcm_file = row[2]
    province = row[3]
    census_div = row[4]
    census_subdiv = row[5]
    
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
    
    # Format the geographic info for display
    province_str = ', '.join(province) if province else 'None'
    census_div_str = ', '.join(census_div) if census_div else 'None'
    census_subdiv_str = ', '.join(census_subdiv) if census_subdiv else 'None'
    
    # Create rectangle with geographic info in popup
    folium.Rectangle(
        bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
        popup=f"<b>Province:</b> {province_str}<br><b>Census Division:</b> {census_div_str}<br><b>Census Subdivision:</b> {census_subdiv_str}",
        color=color,
        fill=True,
        fillOpacity=0.3,
        weight=2
    ).add_to(m)

# Add a legend
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

m.save('./data/outputs/bbox_map.html')
