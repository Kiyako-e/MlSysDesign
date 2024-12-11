import requests
import zipfile
from pathlib import Path
from utils.logger import logger

def load_data():
    url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    zip_path = "ml-latest-small.zip"
  
    response = requests.get(url)
    extract_path = Path("data")
    
    with open(zip_path, "wb") as file:
        file.write(response.content)
      
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
      
    logger.info("Succeed to load data")
