# Weather Server
This repo has the code for the data ingest, pre-processing and serving pipeline for weather data.

Discussion of this service on slack can be found at https://agstackfoundation.slack.com/archives/C025EFCNE0K

## Data
The basic idea is that a user can use this service to get a weather history for any location that we serve
using a fairly simple API. The client side of this API may make some basic decisions about what kind of data
to get and may retrieve data in a slightly more complex form than the user wants, but the final result will
still look like a pretty simple data frame of historical weather data.

The data for this service comes from several sources with the intent of providing the best data we can. In
some areas such as the US and parts of Europe data from weather stations, radars and other sensors are
aggregated and models are used to re-process that source source to get a very detailed grid of estimated 
values with very high quality even in the face of missing data or bad calibrations. In other areas, we
may have to make do with lower resolution data such as raw weather station data, but the goal is always 
to provide data that is as good as we can reasonably provide.

Note that the coverage of this service is pretty much limited to data of agricultural interest. As such,
weather at sea, wave heights and high altitude weather patterns aren't likely to be available through 
this system.

Please also note that if you are at all sophisticated in meteorological data, please jump right in. We
are happy to get all the help we can get.

## Data sources
Here are some comments from the slack channel by Rich Hitchcock who is contributed a lot of effort
for this weather system:

1. We use Multi-Sensor QPE Pass 2:  https://vlab.noaa.gov/web/wdtd/-/multi-sensor-qpe.  It has 2-hour latency compared to Pass 1's 1-hour latency.  However, we discovered issues with MRMS prior to version 12, and we decided that getting higher-quality data was worth the wait.
1. I download hourly data from Iowa State.  Note that some hourly data is missing.  For example, June 22, 2021 is missing hours 14:00 through 17:00:  https://mtarchive.geol.iastate.edu/2021/06/22/mrms/ncep/MultiSensor_QPE_01H_Pass2/  For missing data, the API substitutes NLDAS-2 precipitation.  (Missing hours are stored in a database table named mrmsmissing.)  June 22 was an anomaly – most days have no missing data.  Thus far this calendar year, there have been 12 hours of missing data.
1. MRMS data has 1km x 1km resolution, meaning each GRIB file covering the US contains approx. 9 million points.  To reduce strain on the database, I store only detectable precipitation.  Therefore, missing precipitation is assumed to be 0 -- except for hours listed in the mrmsmissing table, in which case we use NLDAS-2, as mentioned above.
1. I use NOAA's Weather and Climate Toolkit (WCT) to extract the data.  I started out using Python, but WCT is much faster.  WCT was suggested by David Holcomb in response to a Stack Overflow post I had made on the subject.  He is an expert on MRMS, and I may bring him into this discussion.

Once we get the GRIB-formatted data, it looks like this:
```csv
value,datetime,latitude,longitude
0.3,2021-07-07 22:00:00,52.9950,-124.7250
0.3,2021-07-07 22:00:00,52.9950,-124.7150
0.3,2021-07-07 22:00:00,52.9950,-124.4750
0.3,2021-07-07 22:00:00,52.9950,-124.4650
0.4,2021-07-07 22:00:00,52.9950,-124.4550
```
The value here is precipitation. The NLDAS-2 data has different values:
```csv
date,lat,lon,air_temperature,humidity,pressure,zonal_wind_speed,meridional_wind_speed,longwave_radiation,convective_precipitation,potential_energy,potential_evaporation,precipitation,shortwave_radiation,frost
08/01/2021 00:00,49.875,-125.000,27.54000000000002,0.0157792,101312.61,-1.35,0.28,384.96,0.0,675.584,0.477,0.0,579.584,False
08/01/2021 00:00,49.750,-125.000,27.930000000000007,0.01531,101296.61,-0.84,0.2,385.33,0.0,557.056,0.5156,0.0,564.832,False
08/01/2021 00:00,49.625,-125.000,27.879999999999995,0.0135228,99094.37,-0.69,0.18,376.52,0.0,350.848,0.5746,0.0,551.968,False
```
## Pre-processing
The current University of Georgia weather server that Rich and his associates created uses Postgres, but this appears to be 
a complex system to maintain due to the scale. There are roughly 9 million grid points in the MRMS data across the US and 
these nearly 200 hourly values every week. This means that we get billions of data points pretty quickly.

Another alternative is to just take the data and partition it by time and space into a large Parquet dataset. We can then
depend on the ability to read Parquet very fast to retrieve the pertinent data. In several experiments, we tried bucketing
data points into 1 x 1 degree boxes in latitude and longitude. Over a few days, this typically results in about 1500 partitions
in the Parquet data. A history for a single data location can be queried in a few tens of milli-seconds which appears to be
competitive with the more complex Postgres alternative.

More experiments are needed before we can commit to one alternative or the other.

## Serving
... still working on this section. Please feel free to contribute ...
