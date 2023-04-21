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
import copy
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

village_layer = gpd.read_file('merged_villages_comp_group_all_p.shp')
# *********************************************
# track_0 = village_layer[['fid','group_1_1']].groupby('group_1_1').agg(lambda tdf: tdf.unique().tolist())
# track_0 = track_0.reset_index()
# track_0 = track_0["fid"].apply(pd.Series)
# track_0.to_csv("tracking_fid_00.csv")
print(len(village_layer))
village_layer = village_layer.dissolve(by=['group_1_1'], as_index=False, aggfunc={'fid':'min', 'index':'min','FID_Settle': 'min', 'relative_e': 'min', 'num_respon':'sum', 'num_landow':'sum', 'PROVINCE_C': 'first', 'DISTRICT':'first', 'DISTRICT_C':'first', 'TEHSIL':'first', 'TEHSIL_C':'first', 'FULL_NAM_2':'first', 'group_1':'first'})
print(len(village_layer))
print(village_layer.columns)
# # *********************************************
# # track_0 = village_layer[['fid','group_1']].groupby('group_1').agg(lambda tdf: tdf.unique().tolist())
# # track_0 = track_0.reset_index()
# # track_0 = track_0["fid"].apply(pd.Series)
# # track_0.to_csv("tracking_fid_00.csv")
# village_layer = village_layer.dissolve(by=['group_1'], as_index=False, aggfunc={'fid':'min', 'index':'min','FID_Settle': 'min', 'relative_e': 'min', 'num_respon':'sum', 'num_landow':'sum', 'PROVINCE_C': 'first', 'DISTRICT':'first', 'DISTRICT_C':'first', 'TEHSIL':'first', 'TEHSIL_C':'first', 'FULL_NAM_2':'first'})
# print(village_layer.columns)
print( isinstance(village_layer.index, pd.MultiIndex) )
village_layer = village_layer[ ~(village_layer['geometry'] == None) ]
village_layer.set_crs('epsg:4326')
# village_layer.drop(columns=['level_0','level_1'], inplace=True)
cs = village_layer.columns
village_layer['center'] = village_layer.representative_point()
village_layer['center'] = village_layer['center'].to_crs('epsg:24313')
village_layer.reset_index(drop=True, inplace=True)

def merg_update(v, threshold, max_low_threshold):
    num_low = len(v[v['num_respon']<threshold])
    #num_low > max_low_threshold and
    squares = [ [x] for x in v['fid'] ]
    track_d = {'fid': v['fid'] , 'child': squares}
    track_fid = pd.DataFrame(track_d)

    while(len(v['fid'].unique())>49463 ):
        print(len(v['fid'].unique()))
        # low_v = v[v['num_respon']<threshold].drop_duplicates(subset = ['TEHSIL'], keep ='min')
        low_v = v[v['num_respon']<threshold].groupby('TEHSIL', group_keys=False, as_index=False).apply(lambda x: x.loc[x.num_respon.idxmin()])
        for i in range(len(low_v)):
            c = low_v.iloc[i]['center']
            v_within = v.loc[v['TEHSIL'] == low_v.iloc[i]['TEHSIL'] ]
            v_within['distances'] = v_within['center'].distance(c, align = False)
            v_subset = v_within.nsmallest(2, 'distances')
            if(len(v_subset)>1):
                x = v_subset.dissolve(by=None, as_index=False, aggfunc={'fid':'min', 'index':'min','FID_Settle': 'min', 'relative_e': 'min', 'num_respon':'sum', 'num_landow':'sum', 'PROVINCE_C': 'first', 'DISTRICT':'first', 'DISTRICT_C':'first', 'TEHSIL':'first', 'TEHSIL_C':'first', 'FULL_NAM_2':'first', 'center':'first', 'distances':'first', 'group_1':'first', 'group_1_1':'first'})
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

x, td = merg_update(village_layer, 70, 0)
final_village = gpd.GeoDataFrame(x[cs])
td_v1 = td["child"].apply(pd.Series)
td_v1.to_csv("tracking_fid.csv")
final_village.to_file("merged_villages_comp_dis_70.shp")
print(len(x))

# def merg_update(v, threshold, max_low_threshold):
#     num_low = len(v[v['num_respon']<threshold])
#     #num_low > max_low_threshold and
#     gc = 1
#     while(len(v['group_12'].unique())>49463 ):
#         print(len(v['group_12'].unique()))
#         # low_v = v[v['num_respon']<threshold].drop_duplicates(subset = ['TEHSIL'], keep ='min')
#         low_v = v[v['num_respon']<threshold].groupby('TEHSIL', group_keys=False, as_index=False).apply(lambda x: x.loc[x.num_respon.idxmin()])
#         for i in range(len(low_v)):
#             c = low_v.iloc[i]['center']
#             v_within = v.loc[v['TEHSIL'] == low_v.iloc[i]['TEHSIL'] ]
#             v_within['distances'] = v_within['center'].distance(c, align = False)
#             v_subset = v_within.nsmallest(2, 'distances')
#             # whether distance or touch or overlap is a question to answer by the end of this.
#             if(len(v_subset)>1):
#                 if(v.loc[ v['fid'].isin(v_subset['fid']), 'group_2'] == 0 ):
#                     v.loc[ v['fid'].isin(v_subset['fid']), 'group_2'] = gc
#                     gc += 1
#                 elif(v.loc[ v['fid'] == v_subset.iloc[0]['fid'], 'group_2'] == 0 & v.loc[ v['fid'] == v_subset.iloc[1]['fid'], 'group_2'] != 0 ):
#                     v.loc[ v['fid'] == v_subset.iloc[0]['fid'], 'group_2'] = v.loc[ v['fid'] == v_subset.iloc[1]['fid'], 'group_2']
#                 elif(v.loc[ v['fid'] == v_subset.iloc[0]['fid'], 'group_2'] != 0 & v.loc[ v['fid'] == v_subset.iloc[1]['fid'], 'group_2'] == 0):
#                     v.loc[ v['fid'] == v_subset.iloc[1]['fid'], 'group_2'] = v.loc[ v['fid'] == v_subset.iloc[0]['fid'], 'group_2']
#                 else:
#                     if(v_subset.iloc[0]['fid'])
#                 print(v_subset)
#                 print(x)
#                 x.reset_index( level=[0,1], drop=True, inplace=True)
#                 x['center'] = x.representative_point()
#                 x['center'] = x['center'].to_crs('epsg:24313')
#                 v.drop(v[v['fid'].isin(v_subset['fid'])].index, inplace=True)
#                 v = pd.concat([v,x[v.columns]], ignore_index=True)
#                 v.reset_index(drop=True, inplace=True)
#     return v
#
# x = merg_update(village_layer, 70, 0)
# final_village = gpd.GeoDataFrame(x[cs])
# # final_village.to_file("merged_villages_comp_dis_70.shp")
# # print(len(x))
