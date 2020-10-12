import datetime

from flask import Flask, request, render_template
from config import config

from db_utils import *
from weather_utils import get_weather_forecast_df, get_historical_weather_summary
from geo_utils import calculate_path_distance

app = Flask(__name__)


@app.route('/')
def index():
    loc = request.args.get('loc')
    if loc is None:
        lat, lng = config.DEFAULT_MAP_CENTER
    else:
        lat, lng = tuple(map(float, loc.split(',')))
    
    airspace_points = check_nearby_pts_to_pt(config.AIRSPACE_TABLE, (lat, lng), config.HOME_MAP_AIRSPACE_QUERY_RADIUS)
    airspace_info = airspace_points[config.AIRSPACE_TABLE_SHOW_COLUMNS].to_dict('records')
    return render_template('index.html', lat=lat, lng=lng, title='Plan your drone flight', airspace_info=airspace_info)


@app.route('/check-path', methods=['POST'])
def check_path():
    date = datetime.strptime(request.form['trip_date'], config.DATE_FORMAT)
    path = [
        tuple([float(pt.strip()) for pt in pts.split(',')]) for pts in request.form['waypoints'].split('|')]
    
    path_str = ' -> '.join([str(t) for t in path])
    path_distance = round(calculate_path_distance(path), 2)
    intersecting_airspaces = check_nearby_pts_intersection_to_path(config.AIRSPACE_TABLE, path, config.RESTRICTED_ZONE_RADIUS)

    if intersecting_airspaces.shape[0]:
        return render_template(
            'check-path-failure.html',airspaces_table=intersecting_airspaces[config.AIRSPACE_TABLE_SHOW_COLUMNS].to_html(),
            path_distance=path_distance, path=path_str, title="Path summary")

    nearest_station_info = get_nearest_station_to_path_with_data(path)
    end_date, start_date = get_offset_dates(date, -365 * 2)
    historical_weather = get_historical_weather(nearest_station_info['station_id'], start_date, end_date)
    weather_forecast = get_weather_forecast_df(nearest_station_info['point'])
    month_end_date, month_start_date = get_offset_dates(date, -30)
    two_year_summary = get_historical_weather_summary(historical_weather, start_date, end_date)
    monthly_summary = get_historical_weather_summary(historical_weather, month_start_date, month_end_date)
    

    return render_template(
        'check-path-success.html',
        path=path_str, trip_date=date.strftime(config.DATE_FORMAT),
        forecast_table=weather_forecast.to_html(),
        monthly_summary=monthly_summary.to_html(),
        yearly_summary=two_year_summary.to_html(),
        path_distance=path_distance,
        title='Your trip details')


if __name__ == "__main__":
    app.run(debug=True, port=5000, host='0.0.0.0')