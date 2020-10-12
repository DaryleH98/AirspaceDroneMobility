from datetime import datetime, timedelta

from config import config
from geo_utils import get_distance

import psycopg2
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(f'postgresql+psycopg2://{config.DB_USER}@{config.DB_HOST}/{config.DB_NAME}')

def get_connection():
    return psycopg2.connect(host=config.DB_HOST, dbname=config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD)

def find_nearest_station_to_geometry(geom):
    query = f'''
    SELECT * FROM {config.STATION_TABLE}
    ORDER BY {config.GEOMETRY_COLUMN} <-> {geom} LIMIT 1;
    '''
    results = engine.execute(query)
    r = results.next()
    station_loc = (r[2], r[3])
    return {
        'station_id': r[1],
        'point': station_loc
    }


def find_nearest_station_to_point(pt):
    geom = f"ST_Transform(ST_SetSRID(ST_GeomFromText( 'Point({pt[1]} {pt[0]})'), 4326),  4326)"
    return find_nearest_station_to_geometry(geom)


def get_postgis_point_string(pt):
    return f"ST_GeomFromText('POINT({pt[1]} {pt[0]})', 4326)"

def get_postgis_line_from_path(path):
    pts = ', '.join([get_postgis_point_string(pt) for pt in path])
    result = f'''
    st_makeline(
        ARRAY[ {pts} ]
        )'''
    return result

def get_nearest_station_to_path(path):
    geom = get_postgis_line_from_path(path)
    return find_nearest_station_to_geometry(geom)

def get_nearest_station_to_path_with_data(path):
    geom = get_postgis_line_from_path(path)
    query = f'''
    SELECT * FROM {config.STATION_TABLE} st, {config.WEATHER_TABLE} hw
    where st.station_id = hw.station_id
    ORDER BY {config.GEOMETRY_COLUMN} <-> {geom} LIMIT 1;
    '''
    results = engine.execute(query)
    r = results.next()
    station_loc = (r[2], r[3])
    return {
        'station_id': r[1],
        'point': station_loc
    }



def get_historical_weather(station_id, start_date, end_date):
    query = f'''
    SELECT * from {config.WEATHER_TABLE} where station_id='{station_id}' and {config.DATE_COL} between '{start_date}' and '{end_date}'; '''
    results = engine.execute(query)
    df = pd.DataFrame(results, columns=results.keys())
    return df



def get_offset_dates(current: datetime, days: int):
    return current.strftime(config.DATE_FORMAT), (current + timedelta(days=days)).strftime(config.DATE_FORMAT)


def check_nearby_pts_intersection_to_path(table_name, path: list, distance_in_m: int):
    query = f'''
    select * FROM {table_name} where st_dwithin(
    {config.GEOMETRY_COLUMN}::geography,
    {get_postgis_line_from_path(path)}::geography,
    {distance_in_m});
    '''
    results = engine.execute(query)
    return pd.DataFrame(results, columns=results.keys())

def check_nearby_pts_to_pt(table_name, point, distance_in_m):
    query = f'''
    select * FROM {table_name} where st_dwithin(
    {config.GEOMETRY_COLUMN}::geography,
    {get_postgis_point_string(point)}::geography,
    {distance_in_m});
    '''
    results = engine.execute(query)
    return pd.DataFrame(results, columns=results.keys())


if __name__ == "__main__":
    p = [(40.78535503247915, -74.01126796173556), (40.782536670898686, -73.98348132626916), (40.796053318729946, -73.95120182762396), (40.77941705375373, -73.9589283033635)]
    ns = get_nearest_station_to_path(p)['station_id']
    end_date, start_date = get_offset_dates(datetime.today(), -365)
    get_historical_weather(ns, start_date, end_date).info()