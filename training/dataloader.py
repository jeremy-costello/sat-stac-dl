import duckdb
import torch
from torch.utils.data import Dataset, DataLoader
import rasterio
import numpy as np


# can scale up with data sharding
class RcmArdDataset(Dataset):
    def __init__(self,
            db_path: str = "./data/outputs/rcm_ard.duckdb",
            table: str = "rcm_ard_tiles",
            filepath_col: str = "filepath",
            transform=None):
        self.conn = duckdb.connect(db_path, read_only=True)
        query = f"SELECT {filepath_col} FROM {table} WHERE {filepath_col} IS NOT NULL"
        self.filepaths = [row[0] for row in self.conn.execute(query).fetchall()]
        self.transform = transform

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx):
        fp = self.filepaths[idx]

        # Load GeoTIFF using rasterio
        with rasterio.open(fp) as src:
            arr = src.read()  # shape: (bands, height, width)
            arr = arr.astype(np.float32)

        # Optional transform (e.g. normalization, torch tensor conversion)
        if self.transform:
            arr = self.transform(arr)
        else:
            arr = torch.from_numpy(arr)

        return arr, fp


# Example usage
if __name__ == "__main__":
    dataset = RcmArdDataset()
    dataloader = DataLoader(dataset, batch_size=4, shuffle=True)

    for batch, fps in dataloader:
        print(batch.shape)  # e.g. torch.Size([4, bands, height, width])
        print(fps)
        break
