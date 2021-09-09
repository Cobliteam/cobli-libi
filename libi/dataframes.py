from datetime import datetime

import pandas as pd

from libi.utils import get_data, convert_datetime_to_unix_milliseconds


def get_stops_driver_data(fleet_data: dict, start_datetime: datetime, end_datetime: datetime):
    query_params = {
        'begin': convert_datetime_to_unix_milliseconds(start_datetime),
        'end': convert_datetime_to_unix_milliseconds(end_datetime),
        'tz': 'America/Sao_Paulo',
    }
    return get_data(fleet_data, 'herbie-1.1/stats/stops/driver', query_params)


def get_devices_data(fleet_data: dict):
    return get_data(fleet_data, 'herbie-1.1/dash/device', {})


def get_pocs_data(fleet_data: dict, start_datetime: datetime, end_datetime: datetime):
    query_params = {
        'startTimestamp': convert_datetime_to_unix_milliseconds(start_datetime),
        'endTimestamp': convert_datetime_to_unix_milliseconds(end_datetime),
        'limit': 1,
        'offset': 0,
    }

    dataframe = pd.DataFrame()
    while True:
        _df = get_data(fleet_data, 'herbie-1.1/planning/pocs', query_params)
        if _df.empty:
            break

        dataframe = dataframe.append(_df)
        query_params['offset'] += 1

    return dataframe
