import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random
import math
import csv
import seaborn as sns
import matplotlib.colors as colors
import multiprocessing
from multiprocesspandas import applyparallel
import psutil
import os
import io
import glob
import gc
import multiprocess_ as mp
import geopandas as gpd
from shapely.geometry import Point, Polygon
import copy

df_bisp = pd.read_csv("BISP_keyed_comp_merge_merge.csv", delimiter = ',', header = 0)
village_info = pd.read_csv("village_indicators_1.csv", delimiter = ',', header = 0)
village_layer = gpd.GeoDataFrame.from_file('merged_village_indicators_1.shp')

def gini_(array):
    array = array['LAND_AREA_M2'].to_numpy()
    if np.amin(array) < 0:
        array -= np.amin(array)
    array += 0.0000001
    array = np.sort(array)
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0]#number of array elements
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #gini coefficient

df_bisp_f = df_bisp[df_bisp['fid']>0]
df_landgini_all = df_bisp_f[["fid", "LAND_AREA_M2"]].groupby('fid').apply_parallel(gini_,num_processes=multiprocessing.cpu_count())
df_landgini_all.columns = ['ginilandall']
landgini_all = df_landgini_all.reset_index()

bisp_owners_own = df_bisp_f[df_bisp_f['LAND_AREA']>0]
df_landgini_own = bisp_owners_own[["fid", "LAND_AREA_M2"]].groupby('fid').apply_parallel(gini_,num_processes=multiprocessing.cpu_count())
df_landgini_own.columns = ['ginilandown']
landgini_own = df_landgini_own.reset_index()

del df_bisp_f
del bisp_owners_own
gc.collect()

village_layer1 = village_layer.merge(landgini_all, how = 'left', on='fid')
village_layer2 = village_layer1.merge(landgini_own, how = 'left', on='fid')

village_info1 = pd.concat([village_info, village_layer2[['ginilandall', 'ginilandown']]], axis = 1 , ignore_index = True)
village_info1.to_csv('village_indicators_2.csv', header = list(village_info.columns) + ['ginilandall', 'ginilandown'] )
village_layer2 = gpd.GeoDataFrame(village_layer2)
village_layer2.to_file('merged_village_indicators_2.shp')
