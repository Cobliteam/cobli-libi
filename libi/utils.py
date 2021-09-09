import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests

from libi.errors import RetrieveDataError

BASE_URL = 'https://api.cobli.co/'


def get_data(
        fleet_data: dict,
        resource_url: str,
        resource_url_query_params: dict,
) -> pd.DataFrame:
    """
    Return an unified pandas dataframe based on a given resource for specified fleets

    :param fleet_data: dict {'fleet_name': 'api_key'}
    :param resource_url: str - API resource endpoint url
    :param resource_url_query_params: dict - API resource query params
    :return: pd.DataFrame
    """
    dataframe = pd.DataFrame()
    for fleet_name, api_key in fleet_data.items():
        dataframe = dataframe.append(get_specific_data(fleet_name, api_key, resource_url, resource_url_query_params))
    return dataframe


def get_specific_data(
        fleet_name: str,
        api_key: str,
        resource_url: str,
        resource_url_query_params: dict
) -> pd.DataFrame:
    """Return a specific pandas dataframe for a given resource of a specified fleet"""
    headers = {
        'Cobli-Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    query_params = urlencode(resource_url_query_params)
    response = requests.get(f'{BASE_URL}{resource_url}?{query_params}', headers=headers)

    print(f'{resource_url}?{query_params}')

    if response.status_code != 200:
        raise RetrieveDataError(
            f"Não foi possível retornar os dados da frota {fleet_name} "
            f"para o recurso {resource_url}. Status: {response.status_code}"
        )

    response_dict = response.json()
    response_dict['fleet_name'] = fleet_name
    dataframe = pd.DataFrame(response_dict)

    if dataframe.empty:
        return dataframe

    return flatten_nested_json_df(dataframe)


def flatten_nested_json_df(df):
    """Transforms a nested json into a one-dimensional pandas dataframe"""
    df = df.reset_index()

    # search for columns to explode/flatten
    s = (df.applymap(type) == list).all()
    list_columns = s[s].index.tolist()

    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()

    while len(list_columns) > 0 or len(dict_columns) > 0:
        new_columns = []

        for col in dict_columns:
            # explode dictionaries horizontally, adding new columns
            horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}.')
            horiz_exploded.index = df.index
            df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
            new_columns.extend(horiz_exploded.columns)  # inplace

        for col in list_columns:
            # explode lists vertically, adding new columns
            df = df.drop(columns=[col]).join(df[col].explode().to_frame())
            new_columns.append(col)

        # check if there are still dict o list fields to flatten
        s = (df[new_columns].applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

    return df


def convert_datetime_to_unix_milliseconds(date_to_convert: datetime) -> int:
    return int(time.mktime(date_to_convert.timetuple())) * 1000
