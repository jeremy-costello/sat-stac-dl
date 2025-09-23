import asyncio
import aiohttp
import pyarrow as pa
from tqdm.asyncio import tqdm as tqdm_asyncio

# Configuration
MAX_CONCURRENT_REQUESTS = 50
REQUEST_TIMEOUT = 30

async def create_rcm_ard_items_table(con):
    """Creates or replaces the rcm_ard_items table."""
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
    print("✅ Created or replaced table 'rcm_ard_items'.")

async def fetch_rcm_items(session: aiohttp.ClientSession, row_id: int, bbox, semaphore: asyncio.Semaphore):
    """Fetch RCM items for a single bbox."""
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
                'limit': 1000  # Increased to get all features, adjust as needed
            }
            
            url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac/search"
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract all feature IDs
                    feature_ids = [feature['id'] for feature in data.get('features', [])]
                    return {"id": row_id, "rcm_items": feature_ids}
                else:
                    return {"id": row_id, "rcm_items": []}
                    
        except Exception:
            return {"id": row_id, "rcm_items": []}

async def update_rcm_ard_items(con):
    """Fetch RCM items and populate the rcm_ard_items table."""
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

    # Configure session and semaphore
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    # Process all requests concurrently
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_rcm_items(session, row[0], row[1], semaphore) for row in rows]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching RCM items")

    # Insert results into database
    arrow_table = pa.Table.from_pydict({
        "id": [r["id"] for r in results],
        "items": [r["rcm_items"] for r in results],
    })

    con.register("rcm_view", arrow_table)
    await loop.run_in_executor(
        None,
        lambda: con.execute("INSERT INTO rcm_ard_items BY NAME SELECT * FROM rcm_view;")
    )
    
    print(f"✅ Populated table with {len(results)} rows.")
