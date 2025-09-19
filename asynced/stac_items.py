import asyncio
from pystac_client import Client

async def get_rcm_items(bbox, datetime="2019-06-12/2048-01-01"):
    """
    Asynchronously fetch RCM ARD items from EODMS STAC API.
    Returns: list of pystac.Items, or empty list if none found
    """
    def _fetch():
        stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"
        catalog = Client.open(stac_url)
        search = catalog.search(
            collections=["rcm-ard"],
            bbox=bbox,
            datetime=datetime,
            limit=1,
            method="GET"
        )
        return list(search.items())

    items = await asyncio.to_thread(_fetch)
    return items  # always a list, possibly empty

async def get_landcover_items(bbox):
    """
    Asynchronously fetch Landcover items from Geo.ca STAC API.
    Returns: list of pystac.Items, or empty list if none found
    """
    def _fetch():
        stac_url = "https://datacube.services.geo.ca/stac/api"
        catalog = Client.open(stac_url)
        search = catalog.search(
            collections=["landcover"],
            bbox=bbox,
            limit=3,
            method="GET"
        )
        items = list(search.items())
        # sort items by year from ID (optional)
        return sorted(items, key=lambda i: i.id)

    items = await asyncio.to_thread(_fetch)
    return items  # always a list
