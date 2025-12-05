# main.py - The main pipeline
# You'll build this step by step!

import pandas as pd

# Step 1: Just read the CSV file and look at it
# We'll add more steps as we go!

df = pd.read_csv("data/real_estate.csv")

print("✅ File loaded!")
print(f"Total rows: {len(df)}")
print("\nFirst 3 rows:")
print(df.head(3))