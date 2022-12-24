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

village_layer = gpd.GeoDataFrame.from_file('merged_villages.shp')
village_layer = village_layer[~ (village_layer['geometry'] == None)]
village_layer['num_respondents'] = 0
village_layer['num_landowners'] = 0
village_layer['education_avg'] = 0

def function_3(df_cut, df_out):
    # Calculate number of respondents in each village
    # Calculate number of land owners in each village
    # Calculate sum of education levels in each village

    polygon_data = {'FID': village_layer['fid'], 'num_resp_found': village_layer['num_respondents'], 'num_landowners_found': village_layer['num_landowners'], 'education_per_village': village_layer['education_avg']}
    df_village = pd.DataFrame(data = polygon_data)
    for i in range(len(df_cut)):
        v = village_layer[village_layer['DISTRICT'].str.lower() == df_cut.iloc[i]['DISTRICT_NAME'].lower()]
        for j in range(len(v)):
            if ( Point(df_cut.iloc[i]['LONGTITUDE'], df_cut.iloc[i]['LATITUDE']).within(v.iloc[j]['geometry']) ):
                k = df_village[df_village['FID'] == v.iloc[j]['fid']].index
                df_village.loc[k[0],'num_resp_found'] += 1
                df_village.loc[k[0],'num_landowners_found'] += 1 * ( df_cut.iloc[i]['LAND_AREA_M2'] > 0 )
                df_village.loc[k[0],'education_per_village'] += df_cut.iloc[i]['Education_Level']
    df_out = df_village
    return df_village


if __name__ == '__main__':
    p_data = {'FID' : [0] * 8, 'num_resp_found': [0] * 8, 'num_landowners_found': [0] * 8, 'education_per_village': [0] * 8 }
    df_2 = pd.DataFrame(p_data)
    df_survey = pd.read_csv("df_cut_1.csv", delimiter = ',', header = 0)
    df_result = mp.dataframe_multiprocess(function = function_3, data_frame = df_survey, data_frame_2 = df_2)

    #print(df_result.head())
    # print(len(df_result[df_result["education_per_village"]>0]))

    df_result1 = df_result.groupby(['FID']).sum()
    print(len(df_result1))
    df_result1.to_csv('village_indicator_0.csv')
