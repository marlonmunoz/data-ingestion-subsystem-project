import json
import os

def load_config(config_path="config/sources.json"):
    '''
    Load and return the pipeline configuration from JSON file
    '''
    with open(config_path, "r") as f:
        config = json.load(f)
    return config

if __name__ == "__main__":
    config = load_config()
    print(json.dumps(config, indent=2))