# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 22:11:01 2018

@author: Jonathan
"""


import searchtweets, arcpy, yaml, os, tweet_parser, ast, flatten_dict
import pandas as pd
from aylienapiclient import textapi
from datetime import datetime 
from time import sleep


outfolder = arcpy.env.scratchFolder

data = dict(
        search_tweets_api = dict(
            account_type = 'premium',
            endpoint = "https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json",
            consumer_key = "bHtfixh93nVQfirGwfqgfe6fA",
            consumer_secret = "1Cnp5sfkCkg25TaAC8tNxcy5GJVKdxquUwXNM9GJsAYpZhiI1w",
)
        ) 
                
with open(os.path.join(outfolder,'creddata.yml'), 'w') as outfile:
    yaml.dump(data, outfile, default_flow_style=False)        

premium_search_args = searchtweets.load_credentials(os.path.join(outfolder,'creddata.yml'),
                                       yaml_key="search_tweets_api",
                                       env_overwrite=False)        

#rule = searchtweets.gen_rule_payload("(MRT OR train) (fail OR breakdown OR fault OR signal OR fix OR delay OR late) bounding_box:[103.6342 1.1158 103.9888 1.4706]", from_date="2017-01-01",
#                        to_date="2017-12-31 23:59",results_per_call=100) # testing with a sandbox account

rule = searchtweets.gen_rule_payload("(MRT OR train) (fail OR breakdown OR fault OR signal OR fix OR delay OR late) place_country: SG", from_date="2017-01-01",
                        to_date="2017-12-31 23:59",results_per_call=100) # testing with a sandbox account
print(rule)

rs = searchtweets.ResultStream(rule_payload=rule,
                  max_results=2000,
                  max_pages=2,
                  max_requests=20,
                  **premium_search_args)

#print(rs)

tweets = list(rs.stream())

#####
project_name = input('project name or date:') + str('.txt')
tweet_out = os.path.join(outfolder,project_name)
with open(tweet_out, 'w', encoding='utf-8') as outfile:
    for tweet in tweets:
        tweet1 = tweet_parser.tweet.Tweet(tweet)
        print(tweet1, file=outfile)        

#add header to data
src=open(tweet_out,"r",encoding='utf-8')
fline="{data} \n"  #Prepending string
oline=src.readlines()
oline.insert(0,fline)
src.close()
src=open(tweet_out,"w",encoding='utf-8')
src.writelines(oline)
src.close()        
        
#conversion to pandas dataframe 

        
#data = input('drag & drop .txt file')
data = tweet_out
try:
    df = pd.read_csv(eval(data), sep = '} {', encoding = 'utf-8')
except SyntaxError:
    df = pd.read_csv(data, sep = '} {', encoding = 'utf-8')


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
        

pd_df(df)

Dataframe1 = df_result 
# rerun 55:78
#Dataframe2 = df_result 

#merge = Dataframe1.append(Dataframe2)
#                  
#merge1= merge.copy()

#merge1 = merge1.drop_duplicates('text')  

Dataframe1 = Dataframe1.drop_duplicates('text')   

#outpath = input('output filepath:')
outpandas = os.path.join(outfolder, project_name.split('.')[0] + str('_pandas.txt'))

try:
    Dataframe1.to_csv(eval(outpandas))
except SyntaxError:
    Dataframe1.to_csv(outpandas)


#sentiment analysis function 

client = textapi.Client("c631e421", "aadb8fe259efcbecb6a7e9f4cc5855b8")

#df = pd.read_csv(r'C:/Users/Jonathan/Downloads/tweetsforSA - tweetsforSA.csv')
dfs = pd.read_csv(r'C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets_noBB_pandas.txt')
#dfs = pd.read_csv(outpandas)

#def sentiment(dfs):
#    dfs['sentiment_polarity'] = '' 
#    for row in range(len(dfs)):
#        text = str(dfs['text'].get_value(row))
#        sentiment = client.Sentiment({'text': text})
#        dfs['sentiment_polarity'][row] = sentiment 

def sentiment(dfs):
    startTime = datetime.now()
    dfs['sentiment_polarity'] = '' 
    for row in range(0,len(dfs), 60):
        text = str(dfs['text'].get_value(row))
        sentiment = client.Sentiment({'text': text})
        dfs['sentiment_polarity'][row] = sentiment
        sleep(60)
    print(datetime.now() - startTime)    
    
sentiment(dfs)
df1 = pd.concat([dfs.drop(['sentiment_polarity'], axis=1), dfs['sentiment_polarity'].apply(pd.Series)], axis=1)

outpath1 = input('output filepath:')
df1.to_csv(eval(outpath1))

outsentiment = os.path.join(outfolder, project_name.split('.')[0] + str('_sentiment.txt'))

try:
    df1.to_csv(eval(outsentiment))
except SyntaxError:
    df1.to_csv(outsentiment)




        