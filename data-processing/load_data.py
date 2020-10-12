
from datetime import datetime

import glob
import os
import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

from config import config

from config.config import *


engine = create_engine(f'postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/{config.DB_NAME}')
#engine = create_engine(f'redshift+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/{config.DB_NAME}', connect_args={'sslmode': 'prefer'})
histocial_data_files = glob.glob(f'{config.HISTORICAL_DATA_PATH}/*.csv')


# ### Load weather data into db
# 
# Download and keep all the csvs in the path HISTORCAL_DATA_PATH specified in config file.

def load_historical_data():
    print("creating historical data")
    if_exists = 'replace'
    for idx, fpath in enumerate(histocial_data_files):
        cs = 25000
        for cn, df in enumerate(pd.read_csv(fpath, names=['station_id', 'date', 'element', 'value', 'mflag', 'qflag', 'sflag', 'time'], chunksize=cs)):
            print(cn * cs, end=', ')
            df.columns = df.columns.str.lower()
            df['date'] = df['date'].astype('str').apply(lambda x: datetime.strptime(x, "%Y%m%d"))
            df.to_sql(WEATHER_TABLE, engine, if_exists=if_exists, chunksize=cs)
            if_exists = 'append'

def get_connection():
    return psycopg2.connect(host=config.DB_HOST, dbname=config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD)


def get_stations_data(stations_meta_path):
    stations_info = []
    with open(stations_meta_path, 'r', encoding='utf-8') as f:
        content = f.readlines()

    for line in content:
        to_add = [x.strip() for x in line.split(' ') if x]
        stations_info.append(to_add)
    return stations_info

def update_geometry(table_name, lat='lat', lon='lon'):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            drop_existing_col_query = f'ALTER TABLE {table_name} DROP COLUMN IF EXISTS geom'
            cursor.execute(drop_existing_col_query)
            add_geom_col_query = f"ALTER TABLE {table_name} ADD COLUMN geom geometry(POINT, 4326)"
            cursor.execute(add_geom_col_query)
            update_geom_query = f"UPDATE {table_name} SET geom=ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
            cursor.execute(update_geom_query)

def load_stations_data():
    print("Creating stations data")
    stations_info = get_stations_data(STATIONS_DATA_PATH)
    station_cols = ['station_id', 'lat', 'lon', 'elevation']
    station_df = pd.DataFrame([info[:len(station_cols)] for info in stations_info], columns=station_cols)
    station_df['lat'] = station_df['lat'].astype('float')
    station_df['lon'] = station_df['lon'].astype('float')
    station_df['elevation'] = station_df['elevation'].astype('float')
    station_df.to_sql(STATION_TABLE, engine, if_exists='replace')
    update_geometry(STATION_TABLE)

def load_airspace_data():
    print("Creating airspace data")
    airspace_data_url = 'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/FAA_UAS_FacilityMap_Data_V3/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'


    def get_airspace_data():
        response = requests.get(airspace_data_url)
        return response.json()

    airspace_data_json = get_airspace_data()
    airspace_df = pd.DataFrame(
        [feature['attributes'] for feature in airspace_data_json['features']]
        )
    airspace_df['lat'] = airspace_df['LATITUDE'].astype('float')
    airspace_df['lon'] = airspace_df['LONGITUDE'].astype('float') 
    airspace_df.to_sql(AIRSPACE_TABLE, engine, if_exists='replace')
    update_geometry(AIRSPACE_TABLE)


# ### Create indexes


def create_spatial_index(table_name, gemoetry_column='geom'):
    query = f'''
    CREATE INDEX {table_name}_gix ON {table_name} USING GIST ({gemoetry_column});
    CLUSTER {table_name} USING {table_name}_gix;'''

    with get_connection() as conn:
        conn.set_isolation_level(0)
        with conn.cursor() as cursor:
            cursor.execute(query)
def create_index(table_name, cols):
    query = f'''
    create index {table_name}_idx on {table_name} ({','.join(cols)})'''
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)


def create_indexes():
    create_index(WEATHER_TABLE, ['station_id', 'date'])
    create_spatial_index(AIRSPACE_TABLE)
    create_spatial_index(STATION_TABLE)

if __name__ == "__main__":
    load_historical_data()
    load_stations_data()
    load_airspace_data()
    create_indexes()
