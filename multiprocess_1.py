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
import pandas as pd

# Multiprocessing functions with two data frames; updating one based on the other one
# I used bits from caffeinated pandas from Scollay
# https://github.com/scollay/caffeinated-pandas/blob/main/caffeinated_pandas_utils.py

def target_invoke(*args):
    function = args[0]
    batch_files = args[1]
    batch_outs = args[2]

    args1 = []

    df = pd.read_feather(batch_files)
    args1.append(df)
    df_2 = pd.read_feather(batch_outs)
    args1.append(df_2)

    for arg in args[3:]:
        args1.append(arg)

    df_2 = function(*args1)

    # df.to_feather(batch_files, compression='lz4')
    df_2.reset_index(drop=True, inplace=True)
    df_2.to_feather(batch_outs, compression='lz4')
    del df
    del df_2
    gc.collect()

def dataframe_multiprocess(**kwargs):

    # assign function to be performed on your data set, as well as data, how to split and parameters
    function_args = []
    for key, val in kwargs.items():
        if key == 'function':
            function = val
        elif key == 'data_frame':
            df_ = val
        elif key == 'data_frame_2':
            df_2 = val
        elif key == 'splitgroup':
            splitgroup = val
        else:
            function_args.append(val)
    # change number of processes if you don't want to use all your (logical) cores
    procs = multiprocessing.cpu_count()

    if 'splitgroup' not in kwargs or splitgroup == None:
        df_splits = np.array_split(df_ , procs)
        df_2_splits = np.array_split(df_2 , procs)
        temporary_batches = []
        temporary_outputs = []
        for i in range(len(df_splits)):
            temporary_batches.append('temporary_batch_'+str(i)+'.feather')
            temporary_outputs.append('temporary_output_'+str(i)+'.feather')
            df_batches = df_splits[i].reset_index(drop=True)
            df_2_batches = df_2_splits[i].reset_index(drop=True, inplace=False)
            df_batches.to_feather(temporary_batches[i], compression='lz4')
            df_2_batches.to_feather(temporary_outputs[i], compression='lz4')


    else:
        temporary_batches = []
        for i in range(procs):
            temporary_batches.append('temporary_batch_' + str(i) + '.feather')
        # this requires your data to be sorted based on the key column
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
    # remove the large data frame from memory
    del df_
    del df_2
    gc.collect()

    manager = multiprocessing.Manager()
    processes = []

    for i in range(procs):
        process = multiprocessing.Process(target = target_invoke, args = [function, temporary_batches[i], temporary_outputs[i]]+ function_args )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    dfs = []
    for batch_file in temporary_outputs:
        dfs.append(pd.read_feather(batch_file))
        os.remove(batch_file)
    for batch_file in temporary_batches:
        os.remove(batch_file)

    df = pd.concat(dfs, ignore_index=True)
    df = df.reset_index(drop=True)

    return df
