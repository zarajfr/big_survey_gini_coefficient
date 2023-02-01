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

df_bisp = pd.read_csv("BISP_keyed.csv", delimiter = ',', header = 0)
# bisp_owners = df_bisp[df_bisp['LAND_AREA']>0]
bisp_owners = df_bisp

bisp_owners['SUM_Electrical_Assets'] = [0]*len(bisp_owners)
bisp_owners['SUM_Vehicle_Assets'] = [0]*len(bisp_owners)
bisp_owners['HH_times_Land'] = bisp_owners['LAND_AREA_M2'] * bisp_owners['NO_HH_MEMBERS']
bisp_owners['SUM_Electrical_Assets'] = 1.0*( bisp_owners['REFRIGRATOR_P'] + bisp_owners['WASHINGMACHINE_P'] + bisp_owners['FREEZER_P'] + bisp_owners['COOKINGSTOVE_P']+ bisp_owners['COOKINGRANGE_P'] + bisp_owners['AIRCONDITIONER_P'] + bisp_owners['MICROWAVEOVEN_P']+ bisp_owners['AIRCOOLER_P'] + bisp_owners['TV_P'] + bisp_owners['GEASER_P'] + bisp_owners['HEATER_P']) / 11.0
bisp_owners['SUMW_Electrical_Assets'] = 1.0*bisp_owners['NO_HH_MEMBERS']*( bisp_owners['REFRIGRATOR_P'] + bisp_owners['WASHINGMACHINE_P'] + bisp_owners['FREEZER_P'] + bisp_owners['COOKINGSTOVE_P']+ bisp_owners['COOKINGRANGE_P'] + bisp_owners['AIRCONDITIONER_P'] + bisp_owners['MICROWAVEOVEN_P']+ bisp_owners['AIRCOOLER_P'] + bisp_owners['TV_P'] + bisp_owners['GEASER_P'] + bisp_owners['HEATER_P']) / 11.0
bisp_owners['SUM_Vehicle_Assets'] = 1.0*( bisp_owners['CAR_P'] + bisp_owners['TRACTOR_P'] + bisp_owners[ 'SCOOTER_P'] + bisp_owners['MOTORCYCLE_P'] )/4.0
bisp_owners['SUMW_Vehicle_Assets'] = 1.0*bisp_owners['NO_HH_MEMBERS']*( bisp_owners['CAR_P'] + bisp_owners['TRACTOR_P'] + bisp_owners[ 'SCOOTER_P'] + bisp_owners['MOTORCYCLE_P'] )/4.0
bisp_owners["HIGHEST_EDUCATION_0"] = 1.0* bisp_owners["HIGHEST_EDUCATION_0"]
bisp_owners["HIGHEST_EDUCATION_1"] = 2.0*bisp_owners["HIGHEST_EDUCATION_1"]
bisp_owners["HIGHEST_EDUCATION_2"] = 3.0*bisp_owners["HIGHEST_EDUCATION_2"]
bisp_owners["Education_Level"] = bisp_owners[["HIGHEST_EDUCATION_2", "HIGHEST_EDUCATION_1", "HIGHEST_EDUCATION_0"]].max(axis=1)

bisp_owners.rename(columns={"resp_ID": "index", 'FID': 'fid'}, inplace = True)
df_sumL = bisp_owners[["fid", "LAND_AREA_M2"]].groupby('fid').agg({'LAND_AREA_M2': ['sum']})
df_sumL.columns = ['sum_landholding']
df_sumL_r= df_sumL.reset_index()

df_sumHH = bisp_owners[["fid", "NO_HH_MEMBERS"]].groupby('fid').agg({'NO_HH_MEMBERS': ['sum']})
df_sumHH.columns = ['sum_hh']
df_sumHH_r= df_sumHH.reset_index()

df_sumHHL = bisp_owners[["fid", "HH_times_Land"]].groupby('fid').agg({'HH_times_Land': ['sum']})
df_sumHHL.columns = ['wsum_landholding']
df_sumHHL_r = df_sumHHL.reset_index()

df_sumEl = bisp_owners[["fid", "SUM_Electrical_Assets"]].groupby('fid').agg({'SUM_Electrical_Assets': ['sum']})
df_sumEl.columns = ['sum_electrical_assets']
df_sumEl_r = df_sumEl.reset_index()

df_sumElw = bisp_owners[["fid", "SUMW_Electrical_Assets"]].groupby('fid').agg({'SUMW_Electrical_Assets': ['sum']})
df_sumElw.columns = ['wsum_electrical_assets']
df_sumElw_r = df_sumElw.reset_index()

df_sumVl = bisp_owners[["fid", "SUM_Vehicle_Assets"]].groupby('fid').agg({'SUM_Vehicle_Assets': ['sum']})
df_sumVl.columns = ['sum_vehicle_assets']
df_sumVl_r = df_sumVl.reset_index()

df_sumVlw = bisp_owners[["fid", "SUMW_Vehicle_Assets"]].groupby('fid').agg({'SUMW_Vehicle_Assets': ['sum']})
df_sumVlw.columns = ['wsum_vehicle_assets']
df_sumVlw_r = df_sumVlw.reset_index()

df_sumEd = bisp_owners[["fid", "Education_Level"]].groupby('fid').agg({'Education_Level': ['sum']})
df_sumEd.columns = ['sum_education_max']
df_sumEd_r = df_sumEd.reset_index()

# bisp_owners_1 = bisp_owners[bisp_owners['LAND_AREA']>0]
# df_sumLHH = bisp_owners_1[["fid", "NO_HH_MEMBERS"]].groupby('fid').agg({'NO_Land_HH': ['sum']})
# df_sumLHH.columns = ['sum_l_hh']
# df_sumLHH_r= df_sumLHH.reset_index()

village_layer = gpd.GeoDataFrame.from_file('villages_landowners.shp')
village_layer = village_layer[~ (village_layer['geometry'] == None)]
# village_layer['sum_landholding'] = 0.0
# village_layer['wsum_landholding'] = 0.0
# village_layer['sum_hh'] = 0
# village_layer['avg_electrical_assets'] = 0.0
# village_layer['avg_vehicle_assets'] = 0.0
# village_layer['sum_education_max'] = 0.0
village_1 = village_layer.merge(df_sumL_r, on='fid')
village_2 = village_1.merge(df_sumHH_r, on='fid')
village_3 = village_2.merge(df_sumHHL_r, on='fid')
village_4 = village_3.merge(df_sumEl_r, on='fid')
village_5 = village_4.merge(df_sumVl_r, on='fid')
village_7 = village_5.merge(df_sumEd_r, on='fid')
village_8 = village_7.merge(df_sumElw_r, on='fid')
village_6 = village_8.merge(df_sumVlw_r, on='fid')

village_6 = village_6[~ (village_6['num_respon'] == 0)]

village_6['landhold_all_avg'] = 1.0*village_6['sum_landholding']/(1.0*village_6['num_respon'])
village_6['wlandhold_all_avg'] = 1.0*village_6['wsum_landholding']/(1.0*village_6['sum_hh'])
village_6['landhold_own_avg'] = 1.0*village_6['sum_landholding']/(1.0*village_6['num_landow'])
village_6['elec_avg'] = 1.0*village_6['sum_electrical_assets']/(1.0*village_6['num_respon'])
village_6['welec_avg'] = 1.0*village_6['wsum_electrical_assets']/(1.0*village_6['sum_hh'])
village_6['veh_avg'] = 1.0*village_6['sum_vehicle_assets']/(1.0*village_6['num_respon'])
village_6['wveh_avg'] = 1.0*village_6['wsum_vehicle_assets']/(1.0*village_6['sum_hh'])
village_6['edu_avg'] = 1.0*village_6['sum_education_max']/(1.0*village_6['num_respon'])

# print( village_6[['landhold_all_avg','wlandhold_all_avg', 'elec_avg', 'welec_avg', 'veh_avg', 'wveh_avg', 'edu_avg']] )
# village_info = village_6[['fid', 'index', 'DISTRICT', 'relative_e', 'num_respon',  'num_landow', 'ratio_land', 'sum_hh', 'sum_landholding', 'landhold_all_avg','wlandhold_all_avg', 'landhold_own_avg', 'elec_avg', 'welec_avg', 'veh_avg', 'wveh_avg', 'edu_avg']]
# village_info.to_csv('village_avg_indicators_0.csv')
