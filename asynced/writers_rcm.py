import asyncio
import itertools
from typing import List, Dict, Any
import pyarrow as pa
from pystac_client import Client
from tqdm.asyncio import tqdm as tqdm_asyncio


# --- Batching configuration ---
BATCH_SIZE = 500  # Adjust based on network/STAC performance


async def create_rcm_ard_items_table(con):
    """
    Creates or replaces the rcm_ard_items table.

    Args:
        con: The DuckDB connection.
    """
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


import asyncio
import itertools
from typing import List, Dict, Any

import pyarrow as pa
from pystac_client import Client
from tqdm.asyncio import tqdm as tqdm_asyncio

# --- Batching configuration ---
BATCH_SIZE = 500  # Adjust based on network/STAC performance

async def update_rcm_ard_items(con):
    """
    Fetches RCM items and populates the rcm_ard_items table.

    Args:
        con: The DuckDB connection.
    """
    loop = asyncio.get_running_loop()
    stac_url = "https://www.eodms-sgdot.nrcan-rncan.gc.ca/stac"

    # 1) Fetch only necessary rows, filtered by landcover_stats, within the same DB
    print("Executing filtered query to fetch rows for update...")
    sql_query = """
        SELECT t.id, t.bbox
        FROM canada_bboxes AS t
        JOIN landcover_stats AS l ON t.id = l.id
        WHERE l.total_count > 0;
    """
    rows_to_update = await loop.run_in_executor(None, lambda: con.execute(sql_query).fetchall())
    print(f"🛰️ Retrieved {len(rows_to_update)} rows for RCM update")

    if not rows_to_update:
        print("✅ No rows to update.")
        return

    # 2) Define batch fetch function for concurrent processing
    def fetch_rcm_sync(batch_bboxes: List[str]) -> List[Dict[str, Any]]:
        """Synchronous function to fetch RCM items for a batch of bboxes."""
        catalog = Client.open(stac_url)
        results = []
        for bbox in batch_bboxes:
            search = catalog.search(
                collections=["rcm-ard"],
                bbox=bbox,
                datetime="2019-06-12/2048-01-01",
                limit=1,
                method="GET"
            )
            items = list(search.items())
            item_ids = [item.id for item in items] if items else []
            results.append({"rcm_items": item_ids})
        return results

    async def fetch_rcm_batched(batch_rows: List[tuple]):
        """Asynchronously process a batch of rows."""
        batch_ids = [row[0] for row in batch_rows]
        batch_bboxes = [row[1] for row in batch_rows]
        
        batch_results = await asyncio.to_thread(fetch_rcm_sync, batch_bboxes)
        
        combined_results = [
            {"id": batch_ids[i], "rcm_items": result["rcm_items"]}
            for i, result in enumerate(batch_results)
        ]
        return combined_results

    # 3) Process batches concurrently
    batches = [
        rows_to_update[i:i + BATCH_SIZE]
        for i in range(0, len(rows_to_update), BATCH_SIZE)
    ]
    
    all_results = list(itertools.chain.from_iterable(
        await tqdm_asyncio.gather(*[fetch_rcm_batched(batch) for batch in batches],
                                  desc="Fetching RCM in batches")
    ))

    # 4) Build PyArrow Table
    arrow_table = pa.Table.from_pydict({
        "id": [r["id"] for r in all_results],
        "items": [r["rcm_items"] for r in all_results],
    })

    # 5) Register + insert into table
    con.register("rcm_view", arrow_table)
    await loop.run_in_executor(
        None,
        lambda: con.execute("""
            INSERT INTO rcm_ard_items BY NAME SELECT * FROM rcm_view;
        """)
    )
    print(f"✅ Populated 'rcm_ard_items' with RCM items for {len(all_results)} rows.")
