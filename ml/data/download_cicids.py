import os
import sys

import requests


def download_cicids2017() -> None:
    """
    Downloads CICIDS2017 dataset from a public URL and saves it to /ml/data/raw/cicids2017.csv.
    """
    # Using a placeholder URL as the actual UNB dataset requires form submission
    # or downloading via specific Kaggle/AWS paths.
    url = "https://media.githubusercontent.com/media/CanadianInstituteForCybersecurity/CIC-IDS-2017/master/MachineLearningCVE/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"

    # Absolute path relative to this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "raw")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "cicids2017.csv")

    print(f"Downloading CICIDS2017 dataset from {url}...")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        percent = int(downloaded_size * 100 / total_size)
                        sys.stdout.write(f"\rDownload progress: {percent}%")
                        sys.stdout.flush()

        print(f"\nSuccessfully downloaded to {output_path}")
    except Exception as e:
        print(f"\nError downloading dataset: {e}")

if __name__ == "__main__":
    download_cicids2017()
