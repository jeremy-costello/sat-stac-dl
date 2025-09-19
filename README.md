# Satellite STAC Downloader

This repository generates random bounding boxes in Canada and downloads **RCM-ARD** imagery at **256×256 pixels** with **30m resolution**. 

In addition to the RCM data, it also downloads:

- **Landcover data**
- **Metadata**:
  - Province
  - Census Division
  - Census Subdivision

A web-based visualization of 1000 example bounding boxes and metadata downloaded using this tool is available here:  
[https://jeremy-costello.github.io/sat-stac-dl/](https://jeremy-costello.github.io/sat-stac-dl/)

**NOTE:** you may have to hard refresh for the base map tiles to appear.
  - Shift + F5 on Chrome
  - Ctrl + Shift + R on Firefox

## Features

- Generates random bounding boxes within Canada.
- Fetches RCM-ARD imagery using STAC APIs.
- Fetches associated landcover data.
- Extracts metadata from shapefiles for provinces, census divisions, and subdivisions.
- Stores results in a database and optionally as GeoTIFFs.
