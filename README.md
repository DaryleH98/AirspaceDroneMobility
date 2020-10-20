# AirspaceDroneWeatherPlanner
AirspaceDroneWeatherPlanner is a project I worked on at Insight Data Science. The project aims at building a batch pipeline for historcial and future weather conditions in controlled and uncontrolled airspace locations across the United States. 


## Table of contents
* [Motivation](README.md#motivation)
* [Architecture](README.md#architecture)
* [Dataset](README.md#dataset)
* [Setup](README.md#setup)
* [Links](README.md#links)

# Motivation

All recreational drone pilots are required by the FAA and LAANC to fly at or below 400 feet above the ground when in uncontrolled airspace at all times. Often times drone operation can often be confusing for both pilots and the communities they fly in. The problem is determining the safest drone operational locations or routes in communities. My solution is to help pilots make an informed decision before planning their route by returning the average of past weather conditions from the last 2 years for a given flight path as well as a future 7 day forecast. 

# Architecture

1. AWS S3 bucket
2. Apache Spark 
3. PostgreSQL(PostGIS)
4. Flask

## Dataset
My data is collected from three sources, one is from Federal Aviation Administration, another is from OpenWeatherMap API and the last is from *[NOAA Global Historical Climatology Network (GHCN)](https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/global-historical-climatology-network-ghcn)*

## Setup 

My Directory tree structure 
```sh
├── app.py
├── config
│   ├── __init__.py
│   ├── config.py
│   └── readme.md
├── data
│   ├── ghcnd-stations.txt
│   ├── historical_weather
│   │   ├── 2019.csv
│   │   └── 2020.csv
├── db_utils.py
├── geo_utils.py
├── load_data.py
├── requirements.txt
├── templates
│   ├── base.html
│   ├── check-path-failure.html
│   ├── check-path-success.html
│   └── index.html
└── weather_utils.py
```

- Update details in config.py (read the comments there for instructions)
- Install requirements
```sh
$ pip install -r requirements.txt
```
### PostgreSQL Database
- 1 Large instance

Step 1: Assign the right Security Groups 

Step 2: Installing postgreSQL

```
$ sudo apt-get update && sudo apt-get -y upgrade
$ sudo apt-get install postgresql postgresql-contrib​
```

Step 3: Run postgreSQL

```
$ sudo -u postgres psql
$ postgres=#\password​
```

Step 4: Load PostGIS extension for postgreSQL

```
$ Create extension postgis;
```

- Run load_data.py in your python interpreter. You can choose to load into local postgres depending on the details you specified in config.py

- Check your db and verify all the three tables are created and populated with data.

- To debug locally, you can run app.py, which will host the app on port 5000 by default
```sh
$ python app.py
```

- In production, you'd probably want to use gunicorn

```sh
$ gunicorn --bind 0.0.0.0:5000 -w 4 wsgi:app
```

# Links
* [Project Demo](http://www.mobilitydata.tech/)
* [Presentation Slides](https://docs.google.com/presentation/d/1R-YObGiUdTsdznviVtARvFIVO0uJW6iQ0EpfcpFmhfw/edit?usp=sharing)
