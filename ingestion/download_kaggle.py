from dotenv import load_dotenv
import os
import kagglehub
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi()
api.authenticate()

load_dotenv()

file_path = os.getenv('local_data_path')

def extract_data_kaggle():
# Download dataset to default cache
    path = kagglehub.dataset_download("datasnaek/youtube-new")

    print("Downloaded to:", path)
    print("Files available:", os.listdir(path))

    # Copy to your local project folder
    target_dir = f"{file_path}"
    os.makedirs(target_dir, exist_ok=True)

    for file in os.listdir(path):
        shutil.copy(
            src=os.path.join(path, file),
            dst=os.path.join(target_dir, file)
        )

    print("Files saved to:", target_dir)