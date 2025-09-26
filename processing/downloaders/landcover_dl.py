import os
import requests
from tqdm import tqdm

def download_tiff(url: str, save_dir: str = "./data/inputs") -> str:
    # Ensure save directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Extract filename from URL
    filename = url.split("/")[-1]
    filepath = os.path.join(save_dir, filename)

    # Stream download with progress bar
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))

    with open(filepath, "wb") as f, tqdm(
        desc=filename,
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive chunks
                f.write(chunk)
                bar.update(len(chunk))

    return filepath

if __name__ == "__main__":
    url = "https://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/land/landcover/landcover-2020-classification.tif"
    tiff_path = download_tiff(url)
    print(f"\nDownloaded file available at: {tiff_path}")
