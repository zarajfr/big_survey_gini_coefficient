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

# df_bisp = pd.read_csv("BISP_keyed_comp_polish.csv", delimiter = ',', header = 0)
# df_bisp = pd.read_csv("BISP_keyed_comp_merge_0.csv", delimiter = ',', header = 0)
df_bisp = pd.read_csv("BISP_keyed_comp_merge.csv", delimiter = ',', header = 0)
# print(df_bisp.columns[53])
# print(df_bisp.columns[54])
print(len(df_bisp))
# df_new_keys = pd.read_csv("tracking_fid_00.csv", delimiter = ',', header = 0)
# df_new_keys = pd.read_csv("tracking_fid.csv", delimiter = ',', header = 0)
df_new_keys = pd.read_csv("tracking_fid_2.csv", delimiter = ',', header = 0)
f = 1
df_new_keys.replace({'': np.nan}, inplace=True)
df_new_keys = df_new_keys.drop(['Unnamed: 0'], axis=1)
df_new_keys = df_new_keys.astype(float)
# print(df_new_keys.columns)
# dtype={'user_id': int}
merged_out = df_bisp[~(df_bisp['fid'].isin(df_new_keys['0'].tolist()))]
l1 = []
l2 = []
u = merged_out['fid'].unique()
# print(list(u).count(9016.0))
# print(df_new_keys[df_new_keys.isin([9016.0]).any(axis=1)])
for i in u :
    x = df_new_keys[df_new_keys.isin([i]).any(axis=1)]
    if ( (len(x) == 1) and (i !=0)):
        l1.append(i)
        y = x['0'].tolist()
        l2.append(y[0])

p_data = {'fid': l1, 'fid_in': l2}
df_r = pd.DataFrame(p_data)

# print(df_r[df_r['fid']==9016])
# print(df_r[df_r['fid']==9013])
print(df_r[df_r['fid']==1029])
# print(df_r[df_r['fid_in']==9016])
# print(df_r[df_r['fid_in']==9013])
print(df_r[df_r['fid_in']==1029])

df_bisp_0 = df_bisp[~(df_bisp['fid'].isin(l1))]
df_bisp_1 = df_bisp[ df_bisp['fid'].isin(l1)]
print(len(df_bisp_1))
df_bisp_1_n = pd.merge(df_bisp_1, df_r, how='left', on = 'fid')
print(df_bisp_1_n[df_bisp_1_n['fid']==1029])
if (f == 0):
    df_bisp_1_n['fid'] = df_bisp_1_n[['fid', 'fid_in']].min(axis=1)
else:
    df_bisp_1_n['fid'] = df_bisp_1_n['fid_in']
df_bisp_1_n.drop('fid_in', axis=1, inplace=True)
print(len(df_bisp_1_n))
df_x = pd.concat([df_bisp_1_n,df_bisp_0], ignore_index=True)
print(len(df_x))
# df_x.to_csv('BISP_keyed_comp_merge_0.csv')
# df_x.to_csv('BISP_keyed_comp_merge.csv')
df_x.drop(columns=['Unnamed: 0.5', 'Unnamed: 0.4', 'Unnamed: 0.3', 'Unnamed: 0.2','Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1', 'Unnamed: 0.1.1.1'], axis=1, inplace=True)
df_x.to_csv('BISP_keyed_comp_merge_merge.csv')
print(df_x.columns)
# print(len(df_x[df_x['fid']==9013.0]))
# print(len(df_x[df_x['fid']==9021.0]))
# print(len(df_x[df_x['fid']==9016.0]))
print(len(df_x[df_x['fid']==1029.0]))
# print(len(df_bisp[df_bisp['fid']==9013.0]))
# print(len(df_bisp[df_bisp['fid']==9021.0]))
# print(len(df_bisp[df_bisp['fid']==9016.0]))
