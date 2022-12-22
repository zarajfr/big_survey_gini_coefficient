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
from shapely.geometry import Point, Polygon


def function_3(df_cut):
    # Calculate number of respondents in each village
    # Calculate number of land owners in each village

    for i in range(len(df_cut)):
        for j in range(len(village_layer)):
            if ( Point(df_cut.iloc[i]['LONGTITUDE'], df_cut.iloc[i]['LATITUDE']).within(village_layer.iloc[j]['geometry']) ):
                village_layer.iloc[j]['num_respondents'] += 1
                # village_layer.iloc[j]['num_landowners'] += ( df_cut.iloc[i]['LAND_AREA_M2'] > 0 )


if __name__ == '__main__':

    village_layer = gpd.GeoDataFrame.from_file('merged_villages.shp')
    village_layer = village_layer[~ (village_layer['geometry'] == None)]
    village_layer['num_respondents'] = 0
    village_layer['num_landowners'] = 0

    df_survey = pd.read_csv("df_cut_1.csv", delimiter = ',', header = 0)
    df_result = mp.dataframe_multiprocess(function = function_3, data_frame = df_survey)

    print(df_result.head())
    # print(df_result.columns)
