import numpy as np
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
