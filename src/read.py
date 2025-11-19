import pandas as pd
import requests
import logging
import json

def read_csv(file_path):
    """
    Read data from CSV file into a pandas DataFrame
    
    Parameters:
    - file_path: path to the CSV file
    
    Returns:
    - DataFrame with the CSV data
    """
    try:
        # Read CSV file into DataFrame
        df = pd.read_csv(file_path)
        logging.info(f"Successfully read {len(df)} records from CSV: {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"CSV file not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"CSV file is empty: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Failed to read CSV file {file_path}: {e}")
        raise


def read_json(file_path):
    """
    Read data from JSON file into a pandas DataFrame
    
    Parameters:
    - file_path: path to the JSON file
    
    Returns:
    - DataFrame with the JSON data
    """
    try:
        # Try reading as standard JSON first
        try:
            df = pd.read_json(file_path)
        except ValueError:
            # If that fails, try reading as JSON lines format
            df = pd.read_json(file_path, lines=True)
        
        logging.info(f"Successfully read {len(df)} records from JSON: {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"JSON file not found: {file_path}")
        raise
    except ValueError as e:
        logging.error(f"Invalid JSON format in {file_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to read JSON file {file_path}: {e}")
        raise


def read_api(api_url, headers=None, params=None):
    """
    Read data from REST API endpoint into a pandas DataFrame
    
    Parameters:
    - api_url: URL of the API endpoint
    - headers: optional dictionary of HTTP headers
    - params: optional dictionary of query parameters
    
    Returns:
    - DataFrame with the API response data
    """
    try:
        # Make GET request to the API
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        
        # Raise exception if request failed
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Convert to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            if 'data' in data:
                df = pd.DataFrame(data['data'])
            elif 'results' in data:
                df = pd.DataFrame(data['results'])
            else:
                df = pd.DataFrame([data])
        else:
            raise ValueError(f"Unexpected API response format: {type(data)}")
        
        logging.info(f"Successfully read {len(df)} records from API: {api_url}")
        return df
        
    except requests.exceptions.Timeout:
        logging.error(f"API request timed out: {api_url}")
        raise
    except requests.exceptions.ConnectionError:
        logging.error(f"Failed to connect to API: {api_url}")
        raise
    except requests.exceptions.HTTPError as e:
        logging.error(f"API returned error status: {e}")
        raise
    except json.JSONDecodeError:
        logging.error(f"API response is not valid JSON: {api_url}")
        raise
    except Exception as e:
        logging.error(f"Failed to read from API {api_url}: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    print("Read module loaded successfully. Import and use the functions in your pipeline.")