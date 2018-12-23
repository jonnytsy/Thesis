# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 13:11:59 2018

@author: Jonathan
"""

import pandas as pd 
import ast, flatten_dict,numpy
def pd_df(df):
    for i in range(len(df['{data}'])):
        if i == 0:
            dict1 = df['{data}'].get_value(i)
            df1dict = ast.literal_eval(dict1)
            dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
            df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
            df11 = df1.transpose()
            global df_result 
            df_result = df11.copy()
        else: 
            dict1 = df['{data}'].get_value(i)
            df1dict = ast.literal_eval(dict1)
            dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
            df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
            df11 = df1.transpose()
            df_result = df_result.append(df11)
        



df = pd.read_csv(r'C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets0812(edit).txt', sep = '} {', encoding = 'utf-8')
pd_df(df) 
df = df_result 
df2 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets1213(edit).txt', sep = '} {', encoding = 'utf-8')
pd_df(df2)
df2 = df_result
df3 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets_noBB_pandas.txt', sep = ',', encoding = 'utf-8')
df4 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/generalquerybatch2_pandas.txt', sep = ',', encoding = 'utf-8')
df5 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/collisiontweets_pandas.txt' , sep = ',', encoding = 'utf-8')

merge = df.append([df2.copy(),df3.copy(),df4.copy(),df5.copy()])

merge1 = merge.drop_duplicates('text')   
term = str( '\\'  ) 
merge1.columns = merge1.columns.str.replace(term,'_')

#merge1['unified_text']= ''
#for row in merge1:
#    if merge1['extended_tweet_full_text'].get_value(row)== None:
#        merge1['unified_text'][row]= merge1['text'].get_value(row)
#    else:
#        merge1['unified_text'][row]= merge1['extended_tweet_full_text'].get_value(row)

#merge1 = merge1.fillna('0')    
#merge1['unified_text']= ''
#for row in range(len(merge1)):
#    if len(merge1.iloc[row, 20]) == 0 :
#        merge1['unified_text'][row]= merge1['text'].get_value(row)
#    else:
#        merge1['unified_text'][row]= merge1['extended_tweet_full_text'].get_value(row)


#merge1 = merge1.drop(columns='Unnamed: 0')
#for i in merge1.index():
#    val = merge1.get_value(i,'extended_tweet_full_text')
#    if len(merge1.get_value(i,'extended_tweet_full_text')) < 4:
#        merge1['unified_text'][i]= str(merge1['text'].get_value(i))
#    else:
#        merge1['unified_text'][i]= str(merge1['extended_tweet_full_text'].get_value(i))
        
        
merge1['unified_text']= ''
subset1 = merge1[['id','text', 'extended_tweet_full_text','unified_text']].copy()   
subset1['str_text'] = subset1['text'].astype(str)

subset1['str_extended_tweet_full_text'] = subset1['extended_tweet_full_text'].astype(str)


subset1 = subset1.reset_index()
for i in range(len(subset1)):
    if len(subset1['str_extended_tweet_full_text'].get_value(i)) < len(subset1['str_text'].get_value(i)):
        subset1['unified_text'][i]= str(subset1['text'].get_value(i))
    else:
        subset1['unified_text'][i]= str(subset1['extended_tweet_full_text'].get_value(i))
    
subset1['unified_text'] = subset1['unified_text'].astype(str)

#subset1.to_csv('C:/Users/Jonathan/Desktop/Thesis/unified2212.csv')
