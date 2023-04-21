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
import copy

village_layer = gpd.GeoDataFrame.from_file('villages_landowners.shp')
village_layer = village_layer[ ~(village_layer['geometry'] == None) ]
village_layer_s = village_layer[['fid','FULL_NAM_2', 'TEHSIL']]

def assign_fid(df_c):
    for i in range(len(df_c)):
        village_layer_s0 = village_layer_s[village_layer_s['TEHSIL'] == df_c.iloc[i]['TEHSIL']]
        village_layer_ss = village_layer_s0
        # if(df_c.iloc[i]['TEHSIL'] == 'Summundari'):
        #     extra_tehsil = village_layer_s[village_layer_s['TEHSIL'] == 'Jaranwala']
        #     village_layer_ss = pd.concat([village_layer_s0, extra_tehsil], ignore_index=True)
        fs = village_layer_ss.loc[village_layer_ss['FULL_NAM_2']==df_c.iloc[i]['FULL_NAM_2'], 'fid'].values
        df_c.at[i,'fid1'] = fs.min()
    return df_c

def correct_fid_1(df_0, df_correct_11):
    for i in range(len(df_0)):
        if(df_0.iloc[i]['VILLAGE_TEHSIL'] in df_correct_11['VILLAGE_TEHSIL'].values ):
            print("hi")
            f = df_correct_11.loc[df_correct_11['VILLAGE_TEHSIL'] == df_0.iloc[i]['VILLAGE_TEHSIL'],'fid'].values
            df_0.at[i,'fid'] = f[0]
    return df_0

def correct_fid_2(df_0, df_correct_22):
    for i in range(len(df_0)):
        if( ( df_0.iloc[i]['fid'] == 0 ) and (df_0.iloc[i]['UC_TEHSIL'] in df_correct_22['UC_TEHSIL'].values) ):
            print("hi2")
            f = df_correct_22.loc[df_correct_22['UC_TEHSIL'] == df_0.iloc[i]['UC_TEHSIL'],'fid'].values
            df_0.at[i,'fid'] = f[0]
    return df_0

def recovery_village(df_0):
    for i in range(len(df_0)):
        if(df_0.iloc[i]['VILLAGE_NAME'] in df_correct_1['VILLAGE_NAME'].values ):
            v = df_correct_1.loc[(df_correct_1['VILLAGE_NAME'] == df_0.iloc[i]['VILLAGE_NAME']) & (df_correct_1['TEHSIL_NAME'] == df_0.iloc[i]['TEHSIL_NAME'] ),'FULL_NAM_2'].values
            if(len(v)>0):
                village_name = v[0]
                t = df_correct_1.loc[(df_correct_1['VILLAGE_NAME'] == df_0.iloc[i]['VILLAGE_NAME']) & (df_correct_1['TEHSIL_NAME'] == df_0.iloc[i]['TEHSIL_NAME'] ),'TEHSIL'].values
                t_name = t[0]
                df_0.at[i,'TEHSIL_NAME'] = t_name
                vl = village_layer.loc[ ( village_layer['FULL_NAM_2'] == village_name) & (village_layer['TEHSIL'] == t_name),'fid'].values
                df_0.at[i,'fid'] = vl.min()

    return df_0

def recovery_uc(df_0):
    for i in range(len(df_0)):
        if(df_0.iloc[i]['fid'] == 0 and df_0.iloc[i]['UNIONCOUNCIL_NAME'] in df_correct_2['UNIONCOUNCIL_NAME'].values ):
            u = df_correct_2.loc[(df_correct_2['UNIONCOUNCIL_NAME'] == df_0.iloc[i]['UNIONCOUNCIL_NAME']) & (df_correct_2['TEHSIL_NAME'] == df_0.iloc[i]['TEHSIL_NAME']),'FULL_NAM_2'].values
            if(len(u)>0):
                uc_name = u[0]
                t = df_correct_2.loc[(df_correct_2['UNIONCOUNCIL_NAME'] == df_0.iloc[i]['UNIONCOUNCIL_NAME']) & (df_correct_2['TEHSIL_NAME'] == df_0.iloc[i]['TEHSIL_NAME'] ),'TEHSIL'].values
                t_name = t[0]
                vl = village_layer.loc[ ( village_layer['FULL_NAM_2'] == uc_name) & (village_layer['TEHSIL'] == t_name),'fid'].values
                df_0.at[i,'fid'] = vl.min()

    return df_0

if __name__ == '__main__':

    # df_correct01 = pd.read_csv("sorted_villages_fuzzy_in_tehsil_matching_correct.csv", delimiter = ',', header = 0)
    # df_correct02 = pd.read_csv("extra_sorted_villages_fuzzy_in_tehsil_correct.csv", delimiter = ',', header = 0)
    df_correct01 = pd.read_csv("all_villages_fuzzy_in_tehsil_matching_correct.csv", delimiter = ',', header = 0)
    df_correct = df_correct01[['best_match_score','VILLAGE_NAME','TEHSIL_NAME','FULL_NAM_2','TEHSIL','score','FULL_NAM_2_correct']]


    # df_correct2_01 = pd.read_csv("sorted_uc_fuzzy_in_tehsil_correct.csv", delimiter = ',', header = 0)
    # df_correct2_02 = pd.read_csv("extra_sorted_uc_fuzzy_in_tehsil_correct_2.csv", delimiter = ',', header = 0)
    df_correct2 = pd.read_csv("all_sorted_uc_fuzzy_in_tehsil_correct.csv", delimiter = ',', header = 0)
    # df_correct2 = pd.concat([df_correct2_01, df_correct2_02], ignore_index = False)

    df_correct_1 = df_correct[ ~ ( df_correct['score'].isna() ) ]
    df_correct_2 = df_correct2[ ~ ( df_correct2['Score'].isna() ) ]
    df_correct_1.loc[df_correct_1['FULL_NAM_2_correct'].notna(),'FULL_NAM_2'] = df_correct_1.loc[ df_correct_1['FULL_NAM_2_correct'].notna(),'FULL_NAM_2_correct']
    df_correct_2.loc[df_correct_2['FULL_NAM_2_correct'].notna(),'FULL_NAM_2'] = df_correct_2.loc[ df_correct_2['FULL_NAM_2_correct'].notna(),'FULL_NAM_2_correct']
    df_correct_1['fid1'] = [0]*len(df_correct_1)
    df_correct_2['fid1'] = [0]*len(df_correct_2)

    df_correct_11 = mp.dataframe_multiprocess(function = assign_fid, data_frame = df_correct_1)
    df_correct_22 = mp.dataframe_multiprocess(function = assign_fid, data_frame = df_correct_2)

    df_master = pd.read_csv("BISP_Master_2-004.csv", delimiter = ',', header = 0)
    df_bisp = pd.read_csv("BISP_keyed.csv", delimiter = ',', header = 0)
    df_bisp.rename(columns={"resp_ID": "index", 'FID': 'fid'}, inplace = True)
    x = df_master[~( df_master['index'].isin(df_bisp['index']) )]
    x['fid'] = 0
    bisp_complete = pd.concat([df_bisp, x], ignore_index=False)
    bisp_fid = bisp_complete[ ~(bisp_complete['fid']==0) ]
    df_0_fid = bisp_complete[ bisp_complete['fid']==0 ]

    del df_bisp
    del bisp_complete
    gc.collect()

    df_0_fid['VILLAGE_TEHSIL'] = list(zip(df_0_fid.VILLAGE_NAME, df_0_fid.TEHSIL_NAME))
    df_0_fid['UC_TEHSIL'] = list(zip(df_0_fid.UNIONCOUNCIL_NAME, df_0_fid.TEHSIL_NAME))
    df_correct_11['VILLAGE_TEHSIL'] = list(zip(df_correct_11.VILLAGE_NAME, df_correct_11.TEHSIL_NAME))
    df_correct_22['UC_TEHSIL'] = list(zip(df_correct_22.UNIONCOUNCIL_NAME, df_correct_22.TEHSIL_NAME))

    df1 = pd.merge(df_0_fid[df_0_fid['VILLAGE_TEHSIL'].isin(df_correct_11['VILLAGE_TEHSIL'])],df_correct_11[['VILLAGE_TEHSIL','fid1']], how='left', on = 'VILLAGE_TEHSIL', suffixes = ('_left', '_right'))
    print(len(df1))
    print(df1[['fid','fid1','VILLAGE_TEHSIL']].head())
    df1['fid'] = df1['fid1']

    df2 = pd.merge(df_0_fid[( df_0_fid['UC_TEHSIL'].isin(df_correct_22['UC_TEHSIL']) ) &  ~( df_0_fid['VILLAGE_TEHSIL'].isin(df_correct_11['VILLAGE_TEHSIL']) ) ],df_correct_22[['UC_TEHSIL','fid1']], how='left', on = 'UC_TEHSIL', suffixes = ('_left', '_right'))
    print(len(df2))
    print(df2[['fid','fid1','UC_TEHSIL']].head())
    df2['fid'] = df2['fid1']

    df_x = pd.concat([df1,df2], ignore_index=False)
    print(len(df_x))
    df_0_fid_found = df_0_fid[df_0_fid['index'].isin(df_x['index'])]
    print(len(df_0_fid_found))
    df_0_fid_0 = df_0_fid[~ ( df_0_fid['index'].isin(df_x['index']))]

    df_0_fid_found.drop('fid', axis=1, inplace=True)
    df_0_fid_found_m = pd.merge(df_0_fid_found,df_x[['index','fid']], on='index')
    print(len(df_0_fid_found_m))

    df_com = pd.concat([bisp_fid,df_0_fid_found_m,df_0_fid_0], ignore_index=False)
    print(len(df_com[df_com['fid']==0]))
    df_com.to_csv('BISP_keyed_comp.csv')
    # df_0_fid.loc[df_0_fid['index'].isin(df1), 'fid'] = df1['fid1']
    # df_result_1 = mp.dataframe_multiprocess(function = correct_fid_1, data_frame = df_0_fid, other = df_correct_11 )
    # df_result_2 = mp.dataframe_multiprocess(function = correct_fid_2, data_frame = df_result_1, other = df_correct_22)
    #
    # print(len(df_result_2[df_result_2['fid']==0]))
    # df_result_2.to_csv('BISP_keyed_comp_2.csv')
    # df_complete_x = pd.concat([bisp_fid, df_result_2], ignore_index=False)
    # print(len(df_complete_x[df_complete_x['fid']==0]))
    # df_complete_x.to_csv('BISP_keyed_comp.csv')
#
# df_x = pd.read_csv("x.csv", delimiter = ',', header = 0)
# df_x = df_x.drop_duplicates(subset=['VILLAGE_NAME','TEHSIL_NAME'])
# recovery_village(df_x)
    # df_0_fid['VILLAGE_TEHSIL'] = list(zip(df_0_fid.VILLAGE_NAME, df_0_fid.TEHSIL_NAME))
    # df_0_fid['UC_TEHSIL'] = list(zip(df_0_fid.UNIONCOUNCIL_NAME, df_0_fid.TEHSIL_NAME))
    # df_correct_1['VILLAGE_TEHSIL'] = list(zip(df_correct_1.VILLAGE_NAME, df_correct_1.TEHSIL_NAME))
    # df_correct_2['UC_TEHSIL'] = list(zip(df_correct_2.UNIONCOUNCIL_NAME, df_correct_2.TEHSIL_NAME))
    # print(len(df_0_fid))
    # x = df_0_fid[df_0_fid['VILLAGE_TEHSIL'].isin(df_correct_1['VILLAGE_TEHSIL'])]
    # print(len(x))
    # y = df_0_fid[ ~(df_0_fid['VILLAGE_TEHSIL'].isin(df_correct_1['VILLAGE_TEHSIL']))]
    # z = y[y['UC_TEHSIL'].isin(df_correct_2['UC_TEHSIL'])]
    # print(len(z))
    # df_0_fid['fid'] = df_0_fid.apply(lambda x: df_correct_11.loc[x['VILLAGE_TEHSIL'] == df_correct_11['VILLAGE_TEHSIL'], x['fid']].reset_index(drop=True), axis=1)
    # df_0_fid['fid'] = df_0_fid.apply(lambda x: df_correct_22.loc[x['UC_TEHSIL'] == df_correct_22['UC_TEHSIL'], x['fid']].reset_index(drop=True), axis=1)
# print( df_correct_2.loc[df_correct_2['VILLAGE_NAME']=='chak jano kalan','Score'] )
# df_recovered_1 = df_0_fid[ df_0_fid['VILLAGE_NAME'].isin(df_correct_1['VILLAGE_NAME']) ]
# df_0_fid.loc[ df_0_fid['VILLAGE_NAME'].isin(df_correct_1['VILLAGE_NAME']), 'fid'] = village_layer.loc[village_layer['FULL_NAM_2'] == ]
# df_0_fid.loc[df_0_fid['VILLAGE_NAME'].isin(df_correct_1['VILLAGE_NAME']), 'fid'] = village_layer[village_layer['FULL_NAM_2'] == ]
# df_not_recovered_1 = df_0_fid[ ~ ( df_0_fid['VILLAGE_NAME'].isin(df_correct_1['VILLAGE_NAME']) ) ]
# df_not_recovered_2 = df_not_recovered_1[ df_not_recovered_1['UNIONCOUNCIL_NAME'].isin(df_correct_2['UNIONCOUNCIL_NAME']) ]
# print(len(df_not_recovered_2))
# df_matchings = pd.read_csv('villages_mp_1.csv')
# village_layer = gpd.GeoDataFrame.from_file('villages_landowners.shp')
# village_layer = village_layer[ ~(village_layer['geometry'] == None) ]
# # check for district and tehsil correctly
# for v in vl:
#     df_0_fid.loc[df_0_fid['VILLAGE_NAME']==v,'VILLAGE_NAME_2'] = vl2
#     df_0_fid.loc[df_0_fid['VILLAGE_NAME']==v,'fid'] = vfid
#
# bisp_complete = pd.concat([bisp_fid, df_0_fid], ignore_index=False)
# bisp_complete.to_csv('bisp_complete_keyed.csv')
