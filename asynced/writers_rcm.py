import asyncio
import aiohttp
import pyarrow as pa
from tqdm.asyncio import tqdm as tqdm_asyncio

# Configuration
MAX_CONCURRENT_REQUESTS = 50
REQUEST_TIMEOUT = 30

async def create_rcm_ard_tables(con):
    """Creates or replaces rcm_ard_items and ensures rcm_ard_properties exists."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            CREATE OR REPLACE TABLE rcm_ard_items (
                id INTEGER PRIMARY KEY,
                items TEXT[]
            );
        """)
    )
    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            CREATE TABLE IF NOT EXISTS rcm_ard_properties (
                id TEXT PRIMARY KEY,
                datetime TEXT,
                order_key TEXT
            );
        """)
    )
    print("✅ Tables 'rcm_ard_items' and 'rcm_ard_properties' ready.")

async def fetch_rcm_items(session: aiohttp.ClientSession, row_id: int, bbox, semaphore: asyncio.Semaphore):
    """Fetch RCM items and properties for a single bbox."""
    async with semaphore:
        try:
            # Convert bbox to coordinate list
            if isinstance(bbox, str):
                coords = [float(x.strip()) for x in bbox.split(',')]
            else:
                coords = [float(x) for x in bbox]
            
            # Build request parameters
            params = {
                'collections': 'rcm-ard',
                'bbox': ','.join(map(str, coords)),
                'datetime': '2019-06-12T00:00:00Z/2048-01-01T23:59:59Z',
                'limit': 1000
            }
            
            url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac/search"
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    features = data.get('features', [])
                    feature_ids = [f['id'] for f in features]
                    # Collect feature property info
                    properties = [
                        {
                            "id": f["id"],
                            "datetime": f.get("properties", {}).get("datetime"),
                            "order_key": f.get("properties", {}).get("order_key")
                        }
                        for f in features
                    ]
                    return {"id": row_id, "rcm_items": feature_ids, "properties": properties}
                else:
                    return {"id": row_id, "rcm_items": [], "properties": []}
        except Exception:
            return {"id": row_id, "rcm_items": [], "properties": []}


async def update_rcm_ard_tables(con):
    """Fetch RCM items and populate both tables."""
    loop = asyncio.get_running_loop()
    
    # Get rows to process
    sql_query = """
        SELECT t.id, t.bbox
        FROM canada_bboxes AS t
        JOIN landcover_stats AS l ON t.id = l.id
        WHERE l.total_count > 0;
    """
    
    rows = await loop.run_in_executor(None, lambda: con.execute(sql_query).fetchall())
    print(f"🛰️ Processing {len(rows)} rows")
    
    if not rows:
        print("✅ No rows to update.")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_rcm_items(session, row[0], row[1], semaphore) for row in rows]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching RCM items")

    # Insert into rcm_ard_items
    arrow_table = pa.Table.from_pydict({
        "id": [r["id"] for r in results],
        "items": [r["rcm_items"] for r in results],
    })
    con.register("rcm_view", arrow_table)
    await loop.run_in_executor(
        None,
        lambda: con.execute("INSERT INTO rcm_ard_items BY NAME SELECT * FROM rcm_view;")
    )
    con.unregister("rcm_view")

    # Flatten unique properties for rcm_ard_properties
    all_properties = {}
    for r in results:
        for p in r["properties"]:
            if p["id"] not in all_properties:  # deduplicate
                all_properties[p["id"]] = (p["datetime"], p["order_key"])

    if all_properties:
        ids = list(all_properties.keys())
        datetimes = [all_properties[i][0] for i in ids]
        order_keys = [all_properties[i][1] for i in ids]

        props_table = pa.Table.from_pydict({
            "id": ids,
            "datetime": datetimes,
            "order_key": order_keys
        })
        con.register("props_view", props_table)
        # Insert only new ones
        await loop.run_in_executor(
            None,
            lambda: con.execute("""
                INSERT INTO rcm_ard_properties
                SELECT * FROM props_view
                ON CONFLICT (id) DO NOTHING;
            """)
        )
        con.unregister("props_view")
        print(f"✅ Added {len(ids)} new properties to 'rcm_ard_properties'.")

    print(f"✅ Populated 'rcm_ard_items' with {len(results)} rows.")
