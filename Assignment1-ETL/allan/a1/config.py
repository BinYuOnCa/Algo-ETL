import json
from pathlib import Path
from dotenv import load_dotenv
import os

# Import .env variables
load_dotenv()

# Settings options
available_timeframes = ['60', '30', '15', '5', '1']
use_sandbox = False
settings_json = {}

# Logs 
home = os.path.expanduser("~")
logs_folder = f"{home}/logs"

# Import settings json file
path = Path(__file__).parent / "config.json"
try:
    with path.open() as f:
        settings_json = json.load(f)

except Exception as e:
    print(e)


