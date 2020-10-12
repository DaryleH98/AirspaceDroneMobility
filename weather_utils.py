import  datetime

import pandas as pd
import requests

from config import config

def get_weather_forecast(point):
    # Only returns forecast for the next 7 days
    url = f'https://api.openweathermap.org/data/2.5/onecall?lat={point[0]}&lon={point[1]}&exclude=minutely&appid={config.OWM_API_KEY}'
    resp = requests.get(url)
    return resp.json()


def get_weather_forecast_df(point):
    weather_json = get_weather_forecast(point)
    weather_df = pd.json_normalize(get_weather_forecast(config.DEFAULT_MAP_CENTER)['daily'])
    weather_df['dt'] = weather_df['dt'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime(config.DATE_FORMAT))
    cols_to_select = ['dt', 'pressure', 'humidity', 'dew_point', 'wind_speed', 'wind_deg', 'clouds', 'temp.min', 'temp.max']
    selected_weather_df = weather_df[cols_to_select]
    return selected_weather_df


def get_historical_weather_summary(df, start_date, end_date, date_col='date'):
    subset_df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]

    if subset_df.shape[0]== 0:
        return pd.DataFrame([])

    trans_df = pd.get_dummies(subset_df, columns=['element'], prefix='')
    desired_metrics = [
        'TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD', 'AWND', 'WDF2', 'WDF5',
        'WSF2', 'WSF5'
    ]
    dummy_cols = [col for col in trans_df.columns if col.startswith('_')]
    cols_to_drop = ['value', 'index', 'time']

    for col in dummy_cols:
        if col.split('_')[1] not in desired_metrics:
            cols_to_drop.append(col)
        else:
            trans_df[col] = trans_df[col] * trans_df['value']

    trans_df = trans_df.drop(cols_to_drop, axis=1)
    trans_df = trans_df.groupby(['station_id', 'date']).sum()

    return trans_df.describe().round(2)