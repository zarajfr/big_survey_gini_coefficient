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

def function_4(df_cut, df_out):
    polygon_data = { 'resp_ID': df_cut['index'], 'FID': [0]* len(df_cut) }
    df_village = pd.DataFrame(data = polygon_data)
    for i in range(len(df_cut)):
        v = village_layer[village_layer['DISTRICT'].str.lower() == df_cut.iloc[i]['DISTRICT_NAME'].lower()]
        for j in range(len(v)):
            if ( Point(df_cut.iloc[i]['LONGTITUDE'], df_cut.iloc[i]['LATITUDE']).within(v.iloc[j]['geometry']) ):
                df_village.loc[i,'FID'] = v.iloc[j]['fid']
    df_out = df_village
    return df_village

if __name__ == '__main__':
    p_data = {'resp_ID' : [0] * multiprocessing.cpu_count() , 'FID' : [0] * multiprocessing.cpu_count() }
    df_2 = pd.DataFrame(p_data)
    df_survey = pd.read_csv("df_cut_2.csv", delimiter = ',', header = 0)
    df_result = mp.dataframe_multiprocess(function = function_4, data_frame = df_survey, data_frame_2 = df_2)

    df_result.to_csv('assigned_to_villages.csv')
