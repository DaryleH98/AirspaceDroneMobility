import os

DB_USER = 'postgres'
DB_HOST = "localhost"
DB_PASSWORD = 'test_password'
DB_NAME = 'airspace'

OWM_API_KEY = '45a250f32ad3e783ed4d5f8669d5f37d' 
DEFAULT_MAP_CENTER = (40.7831, -73.9712)  # lat, lng of Manhattan
RESTRICTED_ZONE_RADIUS = 500 # in Meters
HOME_MAP_AIRSPACE_QUERY_RADIUS = 500000 # in meters

DATA_FOLDER = 'data'
STATIONS_DATA_PATH = os.path.join(DATA_FOLDER, 'ghcnd-stations.txt')
HISTORICAL_DATA_PATH = os.path.join(DATA_FOLDER, 'historical_weather')

WEATHER_TABLE = 'historical_weather'
DATE_COL = 'date'
STATION_TABLE = 'stations'
AIRSPACE_TABLE = 'airspace_data'
GEOMETRY_COLUMN = 'geom'
AIRSPACE_TABLE_SHOW_COLUMNS = ['LATITUDE', 'LONGITUDE', 'GLOBALID', 'ARPT_COUNT', 'APT1_NAME']
DATE_FORMAT = '%Y-%m-%d'
