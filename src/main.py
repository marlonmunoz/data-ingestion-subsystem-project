import pandas as pd
from config import load_config

config = load_config()
source_cfg = config["sources"][0] # Get the first source config
csv_path = source_cfg["path"]

# Read the csv 
df = pd.read_csv(csv_path)
print(df.head())
print(df.columns)