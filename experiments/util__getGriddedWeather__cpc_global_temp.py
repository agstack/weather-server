#!/usr/bin/env python
# coding: utf-8

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd
import pickle
import scipy.stats.stats as st
import uuid
from rasterio import plot as rioplot
from calendar import monthrange
import rasterio.features
from rasterio import plot
from rasterio import warp
from rasterio import mask
from matplotlib import cm
from zipfile import ZipFile
from scipy.interpolate import griddata
from datetime import datetime
from datetime import date
from dateutil.parser import parse
from shapely.geometry import mapping, Point, Polygon
from mpl_toolkits.axes_grid1 import AxesGrid
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from matplotlib.backends.backend_pdf import PdfPages
from numpy import linspace
import geojsoncontour
from shapely import wkt
import dash
import geojsoncontour
import scipy as sp
import scipy.ndimage
import math
import earthpy.plot as ep
import re
import ftplib
from ftplib import FTP
from pathlib import Path
import calendar
from folium import plugins
import folium
import glob
import os
import csv
import urllib 
import shutil
import urllib.request as request
import wget
import shapely
import sentinelhub
import geojson
import json
import shapely.wkt
import rasterio
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
#import shapefile
import urllib
import folium 
import geojson
import sentinelsat
import pyproj
import scipy
import numpy as np
import pandas as pd
import rasterio.shutil
import time
import os, fnmatch
import imageio
import datetime
import plotly.express as px
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_core_components as dcc
import plotly.express as px
import math
import rasterio.features
import rasterio.warp
from rasterio import plot
from rasterio import warp
from rasterio import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.wkt import dumps, loads
from area import area
from geojson import Polygon
from sentinelhub import AwsTile
from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date, timedelta, datetime
from shapely.geometry import shape
from collections import OrderedDict
from sentinelsat import SentinelAPI
import dash_html_components as html
from pyproj import Proj, transform
import plotly.graph_objects as go
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import csv
import re
from IPython.display import HTML
import chart_studio
np.warnings.filterwarnings('ignore')
from geopy.geocoders import Nominatim
#from tobler.area_weighted import area_interpolate
#from tobler.dasymetric import masked_area_interpolate
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import h3
import h3.api.basic_int as h3int
import json
import h3pandas
from matplotlib.tri import Triangulation, LinearTriInterpolator
import rasterio
from shapely import geometry
import multiprocessing
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool

from scipy.interpolate import RectBivariateSpline
from mpl_toolkits.mplot3d import Axes3D

gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
np.warnings.filterwarnings('ignore')
import warnings
warnings.simplefilter(action='ignore', category=Warning)

from matplotlib import pyplot
pyplot.rcParams['figure.dpi'] = 200
from eto import ETo, datasets
from geopandas.tools import sjoin

from raster2points import raster2df
#import torch
import parquet_tools
import pyarrow
#import pyspark
import pyarrow.parquet as pq
import gc


# In[113]:


import netCDF4
import urllib
import requests
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import mpl_toolkits as mp
import sys
#mpl_toolkits.__path__.append('/usr/lib/python3.7/dist-packages/mpl_toolkits/')
#from mpl_toolkits.basemap import Basemap


def getETo(o_df):
   
	et1 = ETo()
	freq = 'D'

	z_msl=o_df['elev'].tolist()
	lat=o_df['lat'].tolist()
	lon=o_df['lon'].tolist()

	p_df = o_df[['TS','T_min','T_max','T_mean']]
	p_df.TS = pd.to_datetime(p_df.TS)
	

	eto1_arr=[]
	eto2_arr=[]
	h3_index__L3=[]
	h3_index__L5=[]
	h3_index__L8=[]
	h3_index__L11=[]
	h3_index__L12=[]

	#print(type(lat))
	#print(lat)

	for i, row in p_df.iterrows():
		#print(i)
		lat_i=lat[i]
		lon_i=lon[i]
		z_msl_i=z_msl[i]

		pp_df = p_df.iloc[[i]]
		pp_df.set_index('TS',drop=True, inplace=True)
		pp_df.index.name='date'
		pp_df = pp_df.astype('float')

		et1.param_est(pp_df,freq,z_msl_i,lat_i,lon_i)
		res_df = et1.ts_param
		#Rn=float(res_df.R_n[0]) #Net radiation (MJ/m2)
		#Rs=float(res_df.R_s[0]) #Incoming shortwave radiation (MJ/m2)

		eto1 = et1.eto_fao()
		eto2 = et1.eto_hargreaves()

		eto_1=eto1[0]
		eto_2=eto2[0]

		eto1_arr.append(eto_1)
		eto2_arr.append(eto_2)

		L3=h3.geo_to_h3(lat_i, lon_i, 3)
		L5=h3.geo_to_h3(lat_i, lon_i, 5)
		L8=h3.geo_to_h3(lat_i, lon_i, 8)
		L11=h3.geo_to_h3(lat_i, lon_i, 11)
		L12=h3.geo_to_h3(lat_i, lon_i, 12)

		h3_index__L3.append(L3)
		h3_index__L5.append(L5)
		h3_index__L8.append(L8)
		h3_index__L11.append(L11)
		h3_index__L12.append(L12)

		#print([lat_i, lon_i, eto_1, eto_2, L5, L8])

	avg_arr = [(eto1_arr[x]+eto2_arr[x])/2 for x in range(0, len(eto1_arr))]
	eto1_arr_in = [(eto1_arr[x] / 25.4) for x in range(0, len(eto1_arr))]
	eto2_arr_in = [(eto2_arr[x] / 25.4) for x in range(0, len(eto2_arr))]
	avg_arr_in = [(avg_arr[x] / 25.4) for x in range(0, len(avg_arr))]

	p_df['Lat'] = lat
	p_df['Lon'] = lon

	p_df['ETo_FAO_MM'] = eto1_arr
	p_df['ETo_HAR_MM'] = eto2_arr
	p_df['ETo_AVG_MM'] = avg_arr

	p_df['ETo_FAO_IN'] = eto1_arr_in
	p_df['ETo_HAR_IN'] = eto2_arr_in
	p_df['ETo_AVG_IN'] = avg_arr_in
		
	p_df['h3_index__L3']=h3_index__L3
	p_df['h3_index__L5']=h3_index__L5
	p_df['h3_index__L8']=h3_index__L8
	p_df['h3_index__L11']=h3_index__L11
	p_df['h3_index__L12']=h3_index__L12 
	
	p_df['YYYY']=o_df['YYYY']
	p_df['MM']=o_df['MM']
	p_df['DD']=o_df['DD']

	p_df.reset_index(drop=True, inplace=True)

	return p_df


def getTile(lat,lon):
	p = Point(lon, lat)
	S_gdf = getAllSentinelScenePolygons()
	tile_list = []
	for i, row in S.gdf.iterrows():
		poly = row.geometry
		tileName=row.TILE
		if poly.contains(p):
			tile_list.append(tileName)
		else:
			continue
	return tile_list


def getGriddedDataForMostRecentDateInYr(vname, yr):
	vnm_df =pd.DataFrame()
	try:
		grid_url = 'https://downloads.psl.noaa.gov/Datasets/cpc_global_temp/'+vname+'.'+yr+'.nc'
		fl = grid_url.split('/')[-1]
		downloadFilename = '/home/sumer/Downloads/'+fl
		r = requests.get(grid_url, allow_redirects=True)
		with open(downloadFilename, 'wb') as f:
			f.write(r.content)
	
		nc = netCDF4.Dataset(downloadFilename)
		#tm = nc.variables['time']
		tms = [(datetime(1900,1,1) + timedelta(hours=x)) for x in nc.variables['time'][:]]
		dt = datetime(datetime.today().year, 1, 1) + timedelta(days=int(tms[-1].strftime('%j')))
		YYYY = str(dt.year)
		MM = str(dt.month).zfill(2)
		DD = str(dt.day).zfill(2) 

		lons = nc.variables['lon'][:]
		lats = nc.variables['lat'][:]
		vnm = nc.variables[vname][:]
		lons_fixed= lons-180
		#(lons + 180) % 360 - 180
		
		vnm_units = (nc.variables[vname].units).replace(' ','')

		nc.close()
		#delete the file
		os.remove(downloadFilename)

		"""
		doy = vnm.shape[0]
		dt_start_of_year = datetime.strptime(yr+'-01-01', '%Y-%m-%d')
		dt_today = dt_start_of_year + timedelta(days=doy)
		dtStr_today = datetime.strftime(dt_today, '%Y-%m-%d')
		YYYY = dtStr_today.split('-')[0]
		MM = dtStr_today.split('-')[1]
		DD = dtStr_today.split('-')[-1]
		"""
		
		vname_yesterday = np.squeeze(vnm[-1,:,:]) #For the last day (yesterday)
		
		vnm_filled = vname_yesterday.filled()
		vnm_filled[vnm_filled>200]=np.nan
		vnm_filled[vnm_filled<-200]=np.nan
		
		vname_full = vname+'__'+vnm_units
		#tmax_units
		
		df = pd.DataFrame()
		idx=0
		for j in range(0,len(lons_fixed)):
			lon=lons_fixed[j]
			for i in range(0,len(lats)):
				lat = lats[i]
				vnm=vnm_filled[i,j]
				if not np.isnan(vnm):
					h3_index__L3= h3.geo_to_h3(lat, lon, 3)
					h3_index__L5 = h3.geo_to_h3(lat, lon, 5)
					h3_index__L8= h3.geo_to_h3(lat, lon, 8)
					h3_index__L11= h3.geo_to_h3(lat, lon, 11)
					h3_index__L12= h3.geo_to_h3(lat, lon, 12)

					#populate a dataframe
					df.loc[idx,vname_full]=vnm
					df.loc[idx,'lat']=lat
					df.loc[idx,'lon']=lon
					df.loc[idx,'h3_index__L3']=h3_index__L3
					df.loc[idx,'h3_index__L5']=h3_index__L5
					df.loc[idx,'h3_index__L8']=h3_index__L8
					df.loc[idx,'h3_index__L11']=h3_index__L11
					df.loc[idx,'h3_index__L12']=h3_index__L12

					idx=idx+1
		#Add the Dt Cols
		df['YYYY']=YYYY
		df['MM']=MM
		df['DD']=DD
		
		vnm_df = df[[vname_full,'lat','lon','h3_index__L3','h3_index__L5','h3_index__L8','h3_index__L11','h3_index__L12','YYYY','MM','DD']] #tmax_df

		
	except Exception as e:
		print(e)

	return vnm_df



def writeNOAAParquet(vnm_df, pa_fileName):
	try:
		print('\n\tWriting Parquet File '+pa_fileName)
		
		vnm_df.to_parquet(
			path=pa_fileName,
			engine='pyarrow',
			compression='gzip',
			partition_cols=['h3_index__L3','h3_index__L5', 'h3_index__L8','h3_index__L11','h3_index__L12','YYYY', 'MM', 'DD'],
		)
		print('\tDone!')
	except Exception as e:
		print(e)
	return 

def loadingBar(count,total,size):
	percent = float(count)/float(total)*100
	sys.stdout.write("\r" + str(int(count)).rjust(3,'0')+"/"+str(int(total)).rjust(3,'0') + ' [' + '='*int(percent/10)*size + ' '*(10-int(percent/10))*size + ']')

# Fill in this for an AGSTACK user
#https://urs.earthdata.nasa.gov/users/XXXXXXX/user_tokens
#username: XXXXXXX
#passwd: YYYYYY
#token='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'

noaa_pa_fileName = '/mnt/md1/NOAA/PARQUET'

try:
	begin_time = time.time()
	print('\n\n\t--- Retrieving cpc_global_temp Data from NOAA ---')
	vname = 'tmin'
	yr = str(datetime.now().year)
	start_time = time.time()
	tmin_df = getGriddedDataForMostRecentDateInYr(vname, yr)
	#print(tmin_df.columns)
	#print(len(tmin_df))

	vname = 'tmax'
	yr = str(datetime.now().year)
	start_time = time.time()
	tmax_df = getGriddedDataForMostRecentDateInYr(vname, yr)
	#print(tmax_df.columns)
	#print(len(tmax_df))

	start_time = time.time()
	new_df = pd.merge(tmax_df, tmin_df,  how='left', left_on=['lat','lon','h3_index__L5', 'h3_index__L8', 'YYYY', 'MM', 'DD'], right_on = ['lat','lon','h3_index__L5', 'h3_index__L8', 'YYYY', 'MM', 'DD'])
	#print(new_df.columns)
	#print(len(new_df))
	print("\t--- %s minutes ---\n\n" % str(round(((time.time() - start_time)/60),2)))

	print('\n\t--- Calculating ETo ---')
	#rename columns for the ETo calcs
	start_time = time.time()
	new_df['tmean__degC']=new_df[['tmin__degC','tmax__degC']].mean(axis=1)
	new_df['TS']=new_df['YYYY'].astype('str')+'-'+new_df['MM'].astype('str')+'-'+new_df['DD'].astype('str')
	new_df.rename(columns={'tmin__degC':'T_min', 'tmax__degC':'T_max', 'tmean__degC':'T_mean'}, inplace=True)
	new_df['elev']=0.0
	new_df['TS'] = pd.to_datetime(new_df['TS'])
	#print(new_df.columns)
	#print(len(new_df))
	#print(new_df.tail(10))

	#get the ETO
	start_time = time.time()

	#print(new_df.columns)
	#print(len(new_df))
	#print(new_df.head(10))

	##########
	pnew_df = getETo(new_df)
	##########

	print(pnew_df.columns)
	#print(len(pnew_df))
	print(pnew_df.head(10))
	print("\t--- %s minutes ---\n\n" % str(round(((time.time() - start_time)/60),2)))

	#write ETo to the parquet file
	print('\n\t--- Writing Parquet File ---')
	
	writeNOAAParquet(pnew_df, noaa_pa_fileName)
	print("\t--- %s minutes ---\n" % str(round(((time.time() - start_time)/60),2)))
	print("\n\n--- %s TOTAL minutes ---\n\n\n" % str(round(((time.time() - begin_time)/60),2)))
except Exception as e:
	print(e)

