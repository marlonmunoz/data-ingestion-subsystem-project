# config.py - Functions to load configuration
# You'll build this step by step!
'''
The config.py module loads our pipeline configuration from JSON file.
It defines a load_config() function that checks if the file exists,
opens it, parses the JSON into a Python dictionary, and returns it. 
This separates configuration from code, makind our pipelines flexible -
we can change settings without modifying the code itlsef.
'''

import json
import os

def load_config(config_path="config/sources.json"):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        return config

if __name__=="__main__":
    config = load_config()
    print("Config loaded successfulkly!")
    print(f"Database URL: {config['defaults']['db_url']}")
    print(f"Number of sources: {len(config['sources'])}")
    print(f"Source name: {config['sources'][0]['name']}")
