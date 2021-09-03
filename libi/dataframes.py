import requests
import pandas as pd
import os
from utils import flatten_nested_json_df
from errors import RetrieveDataError


BASE_URL = 'https://api.cobli.co/'


def get_devices(api_key, current_timestamp, fleet_name=''):
    headers = {
        'Cobli-Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.get(f'{BASE_URL}herbie-1.1/dash/device', headers=headers)

    if response.status_code != 200:
        raise RetrieveDataError("Não foi possível retornar os dados")

    return flatten_nested_json_df(pd.DataFrame(response.json()))
