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
import multiprocess_1 as mp
import geopandas as gpd
from shapely.geometry import Point, Polygon
import copy
from pathlib import Path
from thefuzz import fuzz
from thefuzz import process

# df = pd.read_csv('villages_fuzzy_in_tehsil.csv')
# x = df.sort_values(by=['best_match_score'], ascending=False)
# x.to_csv('sorted_villages_fuzzy_in_tehsil.csv')

# df_t = pd.read_csv('matched_tehsils.csv')
# #went with Summundari instead of Jaranwala
df_t_0 = pd.read_csv('matched_tehsils_tand.csv')
#went with tandiwala itself
l = ['Summundari', 'Tandlianwala']
df_t = df_t_0[df_t_0['TEHSIL'].isin(l)]
village_layer = gpd.GeoDataFrame.from_file('villages_landowners.shp')
village_layer = village_layer[ ~(village_layer['geometry'] == None) ]
village_layer_s = village_layer[['fid','FULL_NAM_2', 'TEHSIL']]

def find_village_matches(df_0_fid_s, matched_results):
    tnn, tn, sc, vn, fd, fn = [], [], [],[],[],[]
    for i in range(len(df_0_fid_s)):
        # print(df_0_fid_s.iloc[i]['TEHSIL_NAME'])
        # print(df_0_fid_s.iloc[i]['VILLAGE_NAME'])
        village_layer_s0 = village_layer_s[village_layer_s['TEHSIL'] == df_0_fid_s.iloc[i]['TEHSIL']]
        village_layer_ss = village_layer_s0
        # if(df_0_fid_s.iloc[i]['TEHSIL'] == 'Summundari'):
        #     extra_tehsil = village_layer_s[village_layer_s['TEHSIL'] == 'Jaranwala']
        #     village_layer_ss = pd.concat([village_layer_s0, extra_tehsil], ignore_index=True)
        y = process.extractOne(df_0_fid_s.iloc[i]['VILLAGE_NAME'], village_layer_ss['FULL_NAM_2'])
        tnn.append(df_0_fid_s.iloc[i]['TEHSIL_NAME'])
        tn.append(df_0_fid_s.iloc[i]['TEHSIL'])
        vn.append(df_0_fid_s.iloc[i]['VILLAGE_NAME'])
        fn.append(y[0])
        sc.append(y[1])
    df_out = {'best_match_score' : sc, 'VILLAGE_NAME': vn, 'TEHSIL_NAME': tnn, 'FULL_NAM_2':fn, 'TEHSIL': tn}
    matched_results = pd.DataFrame(df_out)
    return matched_results


if __name__ == '__main__':
    p_data = {'best_match_score' : [0] * multiprocessing.cpu_count(), 'VILLAGE_NAME': [0] * multiprocessing.cpu_count(), 'TEHSIL_NAME': [0] * multiprocessing.cpu_count(), 'FULL_NAM_2':[0] * multiprocessing.cpu_count(), 'TEHSIL':[0] * multiprocessing.cpu_count() }
    df_result = pd.DataFrame(p_data)
    # # df_survey1 = pd.read_csv("unique_village_names.csv", delimiter = ',', header = 0)
    # df_survey1 = pd.read_csv("extra_unique_village_names.csv", delimiter = ',', header = 0)
    df_survey01 = pd.read_csv("unique_village_names.csv", delimiter = ',', header = 0)
    df_survey02 = pd.read_csv("extra_unique_village_names.csv", delimiter = ',', header = 0)
    df_survey1 = pd.concat([df_survey01,df_survey02], ignore_index= True)
    x = df_t[['TEHSIL_NAME','TEHSIL']]
    print(x)
    l2 = ['summundri', 'tandlian wala']
    df_survey1 = df_survey1[df_survey1['TEHSIL_NAME'].isin(l2)]
    df_survey2 = df_survey1.merge(x, on='TEHSIL_NAME')
    print(df_survey2)
    df_survey3 = df_survey2[~(df_survey2['TEHSIL'].isna() )]
    df_survey=df_survey3[~(df_survey3['VILLAGE_NAME'].isna() )]
    df_result = mp.dataframe_multiprocess(function = find_village_matches, data_frame = df_survey, data_frame_2 = df_result)
    # df_result.to_csv('villages_fuzzy_in_tehsil.csv')
    df_x = df_result.sort_values(by=['best_match_score'], ascending=False)
    # df_x.to_csv('extra_sorted_villages_fuzzy_in_tehsil.csv')
    df_x.to_csv('tand_sorted_villages_fuzzy_in_tehsil.csv')
