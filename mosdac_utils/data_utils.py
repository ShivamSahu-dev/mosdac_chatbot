import requests
import pandas as pd

def _post_req(url, headers, payload, timeout=10):
    """
    Internal helper to perform a POST request and return parsed JSON.

    Parameters
    ----------
    url : str
        Endpoint URL to POST to.
    headers : dict
        HTTP headers for the request.
    payload : dict
        JSON payload to send in the request body.
    timeout : int, optional
        Request timeout in seconds (default 10).

    Returns
    -------
    dict or list or None
        The parsed JSON response on success; None on failure.
    """

    try:
        print("Sending request...")
        response = requests.post(url, headers=headers, json=payload, timeout=timeout, verify=True)
        response.raise_for_status()  # handle all HTTP errors
        print(f"Status Code: {response.status_code}")
        print("Data received.")
        return response.json()
    except Exception as e:
        print("Error during request: ", e)
        return None


# fetches satellite data doesnt work for insitu and radar as they have change in url, headers and payload
def fetch_satellite_data(payload, timeout=10):
    """
    Fetch satellite records from MOSDAC's catalog API.

    This function calls the MOSDAC `getSatelliteData.php` endpoint with the
    provided payload. It works for both Insitu(AWS) and Radar.

    Parameters
    ----------
    payload : dict
        Request payload (as required by the API).
    timeout : int, optional
        Request timeout in seconds (default 10).

    Returns
    -------
    list[dict] or dict or None
        Parsed JSON response from the API, or None if the request failed.
    """

    url = "https://www.mosdac.gov.in/catalog/Search/getSatelliteData.php"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Origin": "https://www.mosdac.gov.in",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    return _post_req(url, headers=headers, payload=payload, timeout=timeout)

def fetch_satellite_sensors_data(satellite_id ,timeout=10):
    """
    Fetch sensor information for a given satellite.
    Note: works only for satellite, Insitu and Radar return sensor info in `fetch_satellite_data` function itself.

    Parameters
    ----------
    satellite_id : str or int
        The unique satellite identifier to query.
    timeout : int, optional
        Request timeout in seconds (default 10).

    Returns
    -------
    list[dict] or dict or None
        Parsed JSON response containing sensor details, or None on failure.
    """

    url = "https://www.mosdac.gov.in/catalog/Search/getSensorData.php"

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "https://www.mosdac.gov.in",
    "X-Requested-With": "XMLHttpRequest"}
    
    payload = {
        "id": str(satellite_id)
    }
    
    return _post_req(url, headers, payload, timeout=timeout)


# a satellite may contain more than one sensor
# data passed is from fetch_sensors_data() its a python list of dict
def process_satellite_sensors_data(satellite_name, satellite_id, data):
    """
    Convert a satellite's sensors JSON into a DataFrame.

    For each sensor in `data`, this function builds a new row containing:
    - satellite_name, satellite_id, sensor_name, sensor_id

    Parameters
    ----------
    satellite_name : str
        Human-readable satellite name.
    satellite_id : str or int
        Satellite identifier.
    data : list[dict]
        Sensor records as returned by `fetch_satellite_sensors_data`.

    Returns
    -------
    pd.DataFrame or None
        DataFrame with one row per sensor (columns: satellite_name, satellite_id, sensor_name, sensor_id). 
        Returns None if `data` is falsy or processing fails.
    """

    if not data:
        print(f"No sensors data for satellite_id {satellite_id}")
        return None
    
    # this will store the value of new dataframe
    new_rows = []
    
    try:
        for sensor in data:
            new_row = {
                "satellite_name": str(satellite_name),
                "satellite_id": str(satellite_id),
                "sensor_name": str(sensor["name"]),
                "sensor_id": str(sensor["id"])
            }
            new_rows.append(new_row)
            
        satellite_sensors_df =  pd.DataFrame(new_rows) 
        return satellite_sensors_df  #return the DataFrame with satellite and sensor data
    except Exception as e:
        print(f"Error processing data for satellite {satellite_id}: {e}")
        return None


def fetch_all_products_data(satellite_id, sensor_id, timeout=10):
    """
    Fetch product data for satellites sensor combination from MOSDAC's catalog API.
    This function calls the MOSDAC `getAllProductData.php` endpoint.
    This function is also encapsulated in `make_all_products_dataframe`.

    Parameters
    ----------
    satellite_id : str or int
        Satellite identifier.
    sensor_id : str or int
        Sensor identifier.
    timeout : int, optional
        Request timeout in seconds (default 10).

    Returns
    -------
    post request to the server via `_post_req` function it uses `requests`.

    """
    
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "https://www.mosdac.gov.in",
    "X-Requested-With": "XMLHttpRequest"}

    url = "https://www.mosdac.gov.in/catalog/Search/getAllProductData.php"
    
    payload = {
        "datasource_id": str(satellite_id), # satellite_id 
        "sensor_id": str(sensor_id)
    }
    
    return _post_req(url, headers, payload, timeout=timeout)



def process_all_products_data(satellite_id, satellite_name, sensor_id, sensor_name ,data): 
    """
    
    Processes product data for a given satellite-sensor combination.
    Adds satellite and sensor details to each product row.
    
    Parameters
    ----------
        satellite_id (str): ID of the satellite.
        satellite_name (str): Name of the satellite.
        sensor_id (str): ID of the sensor.
        sensor_name (str): Name of the sensor.
        data (list[dict]): Product data as list of dictionaries.
    
    Returns
    -------
        pd.DataFrame or None: DataFrame containing product details.
        Returns None if data is invalid or empty.

    """
    
    if not data:
        print(f"No data for satellite_id: {satellite_id} & sensor_id: {sensor_id}")
        return None
    
    product_df = pd.DataFrame(data)
    
    # adding new cols
    product_df["satellite_name"] = satellite_name
    product_df["satellite_id"] = satellite_id
    product_df["sensor_name"] = sensor_name
    product_df["sensor_id"] = sensor_id
    
    # rearrange the cols
    columns = product_df.columns.tolist()
    new_order = ["satellite_name","satellite_id","sensor_name","sensor_id"] + [col for col in columns if col not in ["satellite_name","satellite_id","sensor_name","sensor_id"]] 
    product_df = product_df[new_order]
    
    return product_df 

#capsule function
def make_all_products_dataframe(satellite_sensors_df):
    """
    (Capsule Function)
    Iterate over satellite-sensor pairs, fetch & process product data, and concatenate results into a single new df.

    Parameters
    ----------
    satellite_sensors_df : pd.DataFrame
        Expected columns: satellite_id, satellite_name, sensor_id, sensor_name

    Returns
    -------
    pd.DataFrame
        Concatenated DataFrame of products for all satellite-sensor pairs (empty DataFrame if none).
    """
    
    all_dataframes = []
    
    for idx, row in satellite_sensors_df.iterrows():
        satellite_id = row["satellite_id"]
        satellite_name = row["satellite_name"]
        sensor_name = row["sensor_name"]
        sensor_id = row["sensor_id"]
        
        data = fetch_all_products_data(satellite_id=satellite_id, sensor_id=sensor_id)
        processed_df = process_all_products_data(satellite_id=satellite_id, satellite_name=satellite_name, sensor_id=sensor_id, sensor_name=sensor_name, data=data)
    
        if processed_df is not None:
            all_dataframes.append(processed_df)
        else:
            print(f"dataFrame is None for satellite_id: {satellite_id} , sensor_id:{sensor_id}")
            print("skipping this sensor")
            continue
    
    if not all_dataframes:
        print("No product data found. Returning empty DataFrame.")
        return pd.DataFrame()
    
    all_products_df = pd.concat(all_dataframes, ignore_index=True)
    return all_products_df
