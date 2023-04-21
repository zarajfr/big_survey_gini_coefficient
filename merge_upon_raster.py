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

village_layer = gpd.read_file('merged_villages_comp_dis_70.shp')
village_layer.set_crs('epsg:4326')
village_layer["max_build"] = 0
cs = village_layer.columns
print(cs)
village_layer['center'] = village_layer.representative_point()
village_layer['center'] = village_layer['center'].to_crs('epsg:24313')
village_layer.reset_index(drop=True, inplace=True)
raster_layer = rasterio.open("built_up_raster.tif")
print(type(raster_layer))
print(raster_layer.crs)
val_arr = raster_layer.read(1)
affine = raster_layer.transform
percentile_builtup = rasterstats.zonal_stats(village_layer, val_arr, affine = affine, stats=['percentile_95.0'])
# percentile_builtup = rasterstats.zonal_stats(village_layer, val_arr, affine = affine, stats=['mean'])
l = []
for i in range(len(village_layer)):
     l.append(percentile_builtup[i]['percentile_95.0'])

village_layer["max_build"] = l
print(village_layer["max_build"] )
p_01 = village_layer["max_build"].quantile(0.5)
print(p_01)

def merg_update(v, threshold, q_threshold):
    num_low = len(v[v['num_respon']<threshold])
    #num_low > max_low_threshold and
    squares = [ [x] for x in v['fid'] ]
    track_d = {'fid': v['fid'] , 'child': squares}
    track_fid = pd.DataFrame(track_d)

    while(len(v['fid'].unique())>39690 ):
        print(len(v['fid'].unique()))
        # low_v = v[v['num_respon']<threshold].drop_duplicates(subset = ['TEHSIL'], keep ='min')
        low_v = v[ ( v['num_respon']<threshold ) & (v['max_build']>q_threshold) ].groupby('TEHSIL', group_keys=False, as_index=False).apply(lambda x: x.loc[x.num_respon.idxmin()])
        for i in range(len(low_v)):
            c = low_v.iloc[i]['center']
            v_within = v.loc[v['TEHSIL'] == low_v.iloc[i]['TEHSIL'] ]
            v_within['distances'] = v_within['center'].distance(c, align = False)
            v_subset = v_within.nsmallest(2, 'distances')
            if(len(v_subset)>1):
                x = v_subset.dissolve(by=None, as_index=False, aggfunc={'fid':'min', 'index':'min','FID_Settle': 'min', 'relative_e': 'min', 'num_respon':'sum', 'num_landow':'sum', 'PROVINCE_C': 'first', 'DISTRICT':'first', 'DISTRICT_C':'first', 'TEHSIL':'first', 'TEHSIL_C':'first', 'FULL_NAM_2':'first', 'center':'first', 'distances':'first', 'group_1':'first', 'group_1_1':'first', 'max_build':'max'})
                fid_list = v_subset['fid'].unique()
                fid_1 = filter(lambda a: a != x.iloc[0]['fid'], fid_list)
                fid_list1 = list(fid_1)
                list1 = track_fid.loc[track_fid['fid']== fid_list1[0],'child']
                fid_2 = filter(lambda a: a == x.iloc[0]['fid'], fid_list)
                fid_list2 = list(fid_2)
                list2 = track_fid.loc[track_fid['fid']==fid_list2[0], 'child']
                track_fid.drop(track_fid[track_fid['fid'].isin(fid_list)].index, inplace=True)
                p = list2.item() + list1.item()
                p.insert(0, p.pop(p.index(fid_list2[0])))
                df = pd.DataFrame({"fid":[ fid_list2[0] ], "child":[p]})
                track_fid = pd.concat([track_fid, df], ignore_index=True)
                x.reset_index( level=[0], drop=True, inplace=True)
                x['center'] = x.representative_point()
                x['center'] = x['center'].to_crs('epsg:24313')
                v.drop(v[v['fid'].isin(v_subset['fid'])].index, inplace=True)
                # v = v.append(x[v.columns])
                v = pd.concat([v, x[v.columns]], ignore_index=True)
                v.reset_index(drop=True, inplace=True)
    return v, track_fid

x, td = merg_update(village_layer, 70, p_01)
print(cs)
final_village = gpd.GeoDataFrame(x[cs])
td_v1 = td["child"].apply(pd.Series)
td_v1.to_csv("tracking_fid_2.csv")
final_village.to_file("merged_villages_35_70_95_50.shp")
print(len(x))
