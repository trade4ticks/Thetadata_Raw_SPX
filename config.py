import os
from dotenv import load_dotenv

load_dotenv()

THETADATA_BASE_URL = os.environ.get("THETADATA_BASE_URL", "http://localhost:25503")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
