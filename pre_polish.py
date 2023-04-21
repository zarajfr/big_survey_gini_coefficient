import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random
import math
import csv
import seaborn as sns
import matplotlib.colors as colors
import multiprocessing
import psutil
import os
import io
import glob
import gc
import multiprocess_ as mp
import geopandas as gpd
import pysal
import libpysal
from splot.libpysal import plot_spatial_weights
import fiona
from shapely.geometry import Point
import geopy.distance
from shapely.geometry import Point, Polygon
import rasterio
from rasterio.plot import show
import rasterstats
from rasterstats import zonal_stats
import os
import copy
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
# import warnings
# from pandas.core.common import SettingWithCopyWarning
# warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# villages with the same name but different geomtry - dangerous for BISP

village_layer = gpd.read_file('merged_villages_comp_group_all.shp')
cs = village_layer.columns
df_0_fid = pd.read_csv("BISP_recovered_by_name_first_fid.csv", delimiter = ',', header = 0)
v = village_layer[village_layer['fine_fn']==0.0]
tobe_removed = df_0_fid[df_0_fid['fid'].isin(v['fid'])]
df_bisp = pd.read_csv("BISP_keyed_comp.csv", delimiter = ',', header = 0)
print(len(df_bisp[df_bisp['fid']==0]))
x = df_bisp[df_bisp['index'].isin(tobe_removed['index'])]
x['existt'] = 1
x2 = x[['fid', 'existt']].groupby('fid').agg({'existt':['sum']})
x2.columns = ['existt']
x2 = x2.reset_index()
x2 = x2.merge(village_layer.loc[village_layer['fid'].isin(x2['fid']), ['num_respon', 'fid']], on='fid')
x2['num_respon2'] = x2['num_respon'] - x2['existt']
vx = village_layer.loc[village_layer['fid'].isin(x2['fid'])]
vx = vx.merge(x2[['num_respon2','fid']], on = 'fid')
vx['num_respon'] = vx['num_respon2']
vx = vx.drop(columns=[ 'num_respon2'])
village_layer.drop( village_layer.loc[village_layer['fid'].isin(x2['fid'])].index, inplace = True)
village_layer = pd.concat([village_layer, vx], ignore_index=True)
df_bisp.loc[df_bisp['index'].isin(tobe_removed['index']), 'fid'] = 0
print(len(df_bisp[df_bisp['fid']==0]))
df_bisp.to_csv("BISP_keyed_comp_polish.csv")
village_layer.to_file("merged_villages_comp_group_all_p.shp")
