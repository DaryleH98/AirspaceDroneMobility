import xarray as xr
import cfgrib

ds = xr.open_dataset('test.grib', engine='cfgrib')
#check out the data:
ds.dimensions.keys()
ds.variables.keys()

#retrieve the dimension values:
timeLevel = ds.variables['time'].data
longitude = ds.variables['latitude'].data
latitude = ds.variables['longitude'].data

#Query gridpoint at specific time, latitude and longitude indices:


