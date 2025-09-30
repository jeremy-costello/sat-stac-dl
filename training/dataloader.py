import duckdb
import torch
from torch.utils.data import Dataset, DataLoader
import rasterio
import numpy as np
import torchvision.transforms as T


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

        # Load GeoTIFF
        with rasterio.open(fp) as src:
            arr = src.read()  # (C, H, W)
            arr = arr.astype(np.float32)

        # Convert to torch tensor first
        tensor = torch.from_numpy(arr)  # (C, H, W)

        # Apply torchvision transform if provided
        if self.transform:
            tensor = self.transform(tensor)

        return tensor, fp


if __name__ == "__main__":
      # Torchvision transforms work on CHW tensors now
    transform = T.Compose([
        T.RandomCrop(224),
        T.Normalize(mean=[0.0], std=[1.0])
    ])

    dataset = RcmArdDataset(transform=transform)
    dataloader = DataLoader(dataset, batch_size=4, shuffle=True)

    for batch, fps in dataloader:
        print(batch.shape)  # torch.Size([4, C, 224, 224])
        print(fps)
        break
