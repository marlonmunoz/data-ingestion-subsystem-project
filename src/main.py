import pandas as pd
from config import load_config
from validate import rename_columns, cast_types, drop_missing, apply_rules

config = load_config()
source_cfg = config["sources"][0] # Get the first source config
csv_path = source_cfg["path"]
required_fields = ["data_date", "city", "location_id"] # Adjust as needed

# Read the csv 
df = pd.read_csv(csv_path)
df = rename_columns(df, source_cfg["schema"])
df = cast_types(df, source_cfg["schema"])
df = drop_missing(df, required_fields)
valid_df, rejected_df = apply_rules(df, source_cfg["rules"])
# print(df.head())
# print(df.columns)
# print(df.dtypes)
# print(df.isnull().sum())
print("Valid rows:", len(valid_df))
print("Rejected rows:", len(rejected_df))