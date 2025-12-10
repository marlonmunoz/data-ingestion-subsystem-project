# config.py - Functions to load configuration
# You'll build this step by step!
'''
The config.py module loads our pipeline configuration from JSON file.
It defines a load_config() function that checks if the file exists,
opens it, parses the JSON into a Python dictionary, and returns it. 
This separates configuration from code, makind our pipelines flexible -
we can change settings without modifying the code itlsef.
'''

import json # needed to convert JSON into Python Dictionary
import sys
import os # needed to check if config file exists before tryin to open it

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logs.utils import logger

def load_config(config_path="config/sources.json"):
    if not os.path.exists(config_path): # here we're checking weather the file exist at that path
        raise FileNotFoundError(f"Config file not found: {config_path}") # raise an exception that prevents 
                                                                         # opening a file that doesn't exist
    
    # Using conetext manager: WITH to autimatically close the file when done even if error ocurrs
    with open(config_path, 'r') as the_file: # opens the file for reading in reading mode
        config = json.load(the_file) # converts JSON into a Python Dictionary
        return config

if __name__=="__main__":
    config = load_config()
    logger.info("Config loaded successfulkly!")
    logger.info(f"Database URL: {config['defaults']['db_url']}")
    logger.info(f"Number of sources: {len(config['sources'])}")
    logger.info(f"Source name: {config['sources'][0]['name']}")


'''loads the JSON configuration file and returns it as a Python 
dictionary so the pipeline knows where to find data and how to 
process it.'''