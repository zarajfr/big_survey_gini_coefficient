import numpy as np
import matplotlib.pyplot as plt
import random
from random import shuffle
import math
from pprint import pprint
import csv
import seaborn as sns
import matplotlib.gridspec as gridspec
import matplotlib.colors as colors
import multiprocessing
import psutil
import os
import io
import glob
import gc
# import geopandas as gpd
import pandas as pd

def target_invoke(*args):
    function = args[0]
    batch_files = args[1]

    args1 = []

    df = pd.read_feather(batch_files)
    args1.append(df)

    for arg in args[2:]:
        args1.append(arg)

    df = function(*args1)

    df.to_feather(batch_files, compression='lz4')

    del df
    gc.collect()

def dataframe_multiprocess(**kwargs):

    for key, val in kwargs.items():
        if key == 'function':
            function = val
        elif key == 'data_frame':
            df_ = val
        elif key == 'splitgroup':
            splitgroup = val
        else:
            print("else")

    procs = multiprocessing.cpu_count()

    if 'splitgroup' not in kwargs or splitgroup == None:
        df_splits = np.array_split(df_ , procs)
        temporary_batches = []
        for i in range(len(df_splits)):
            temporary_batches.append('temporary_batch_'+str(i)+'.feather')
            df_batches = df_splits[i].reset_index(drop=True)
            df_batches.to_feather(temporary_batches[i], compression='lz4')

    else:
        temporary_batches = []
        for i in range(procs):
            temporary_batches.append('temporary_batch_' + str(i) + '.feather')
        df_bins = df_[splitgroup].drop_duplicates(keep='first').reset_index(drop=False)
        df_splits = np.array_split(df_bins, procs)
        for i in range(len(df_splits)):
            start_i= df_splits[i].iloc[0]['index']
            if i == len(df_splits)-1:
                end_i = None
            else:
                end_i = df_splits[i+1].iloc[0]['index']
            df_batches = df_[start_i: end_i].reset_index(drop=True)
            df_batches.to_feather(temporary_batches[i], compression='lz4')
            del df_batches
    del df_
    gc.collect()

    manager = multiprocessing.Manager()
    processes = []

    for i in range(procs):
        process = multiprocessing.Process(target = target_invoke, args = (function, temporary_batches[i]))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    dfs = []
    for batch_file in temporary_batches:
        dfs.append(pd.read_feather(batch_file))
        os.remove(batch_file)

    df = pd.concat(dfs, ignore_index=True)
    df = df.reset_index(drop=True)

    return df

def function1(df_cut):
    temp = []
    for i in range(len(df_cut)):
        if (df_cut.iloc[i]["HIGHEST_EDUCATION_2"] == 1):
            temp.append(3)
        else:
            if (df_cut.iloc[i]["HIGHEST_EDUCATION_1"] == 1):
                temp.append(2)
            else:
                if(df_cut.iloc[i]["HIGHEST_EDUCATION_0"] == 1):
                    temp.append(1)
                else:
                    temp.append(0)

    df_cut['Education_Level'] = temp
    return df_cut

if __name__ == '__main__':

    df_survey = pd.read_csv("df_cut.csv", delimiter = ',', header = 0)
    df_result = dataframe_multiprocess(function = function1, data_frame = df_survey)
    print(df_result.head())
