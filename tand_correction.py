import csv
import numpy as np
import pandas as pd

df_correct01 = pd.read_csv("sorted_villages_fuzzy_in_tehsil_matching_correct.csv", delimiter = ',', header = 0)
df_correct02 = pd.read_csv("extra_sorted_villages_fuzzy_in_tehsil_correct.csv", delimiter = ',', header = 0)
df_correct = pd.concat([df_correct01[['best_match_score','VILLAGE_NAME','TEHSIL_NAME','FULL_NAM_2','TEHSIL','score','FULL_NAM_2_correct']], df_correct02[['best_match_score','VILLAGE_NAME','TEHSIL_NAME','FULL_NAM_2','TEHSIL','score','FULL_NAM_2_correct']]], ignore_index = False)
df_rep_01 = pd.read_csv("c_tand_sorted_villages_fuzzy_in_tehsil.csv", delimiter = ',', header = 0)

df_correct2_01 = pd.read_csv("sorted_uc_fuzzy_in_tehsil_correct.csv", delimiter = ',', header = 0)
df_correct2_02 = pd.read_csv("extra_sorted_uc_fuzzy_in_tehsil_correct_2.csv", delimiter = ',', header = 0)
df_correct2 = pd.concat([df_correct2_01, df_correct2_02], ignore_index = False)
df_rep_02 = pd.read_csv("c_tand_sorted_uc_fuzzy_in_tehsil.csv", delimiter = ',', header = 0)

l2 = ['summundri', 'tandlian wala']
x1 = df_correct[ ~ (df_correct['TEHSIL_NAME'].isin(l2) )]
x2 = df_correct[ df_correct['TEHSIL_NAME'].isin(l2) ]
all_v = pd.concat([x1,df_rep_01], ignore_index=True)
all_v.to_csv('all_villages_fuzzy_in_tehsil_matching_correct.csv')

print(len(df_correct2))
x1 = df_correct2[ ~ (df_correct2['TEHSIL_NAME'].isin(l2) )]
x2 = df_correct2[ df_correct2['TEHSIL_NAME'].isin(l2) ]
print(len(x2))
print(len(df_rep_02))
all_uc = pd.concat([x1,df_rep_02], ignore_index=True)
all_uc.to_csv('all_sorted_uc_fuzzy_in_tehsil_correct.csv')
print(len(all_v))
