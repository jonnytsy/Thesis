# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd

data = r'C:\Users\Jonathan\AppData\Local\Temp\scratch\tweets0812.txt'
df = pd.read_csv(data, sep = '} {', header = None)

#for i in df:
#    name = 'dict' + str(i)
    
df1 = pd.DataFrame.from_dict(df[])

df_result = None

for i in range(len(df['{data}'])): 
    if i == 0:
        dict1 = df['{data}'].get_value(i)
        df1dict = ast.literal_eval(dict1)
        dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
        df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
        df11 = df1.transpose()
        df_result = df11.copy()
    else: 
        dict1 = df['{data}'].get_value(i)
        df1dict = ast.literal_eval(dict1)
        dfldict1 = flatten_dict.flatten(df1dict, reducer = 'path')
        df1 = pd.DataFrame.from_dict(dfldict1, orient = 'index')
        df11 = df1.transpose()
        df_result = df_result.append(df11)
        
        
        
df1 = pd.read_csv(r'C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets0812(edit).txt',sep = '} {', encoding= 'utf-8')


import ast, flatten_dict
import pandas as pd
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
df = df_result
df3 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/tweets_noBB_pandas.txt', sep = ',', encoding = 'utf-8')
df4 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/generalquerybatch2_pandas.txt', sep = ',', encoding = 'utf-8')
df5 = pd.read_csv('C:/Users/Jonathan/AppData/Local/Temp/scratch/collisiontweets_pandas.txt' , sep = ',', encoding = 'utf-8')

merge = df.append([df2.copy(),df3.copy(),df4.copy(),df5.copy()])
runfile('C:/Users/Jonathan/Desktop/Thesis/uniify_script.py', wdir='C:/Users/Jonathan/Desktop/Thesis')
merge1 = merge.drop_duplicates('text')

term = str( '\\'  ) 
#for i in merge1.columns():
#    if term in merge1.columns(i):
#        merge1.columns(i) = merge1.columns(i).replace
merge1.columns = merge1.columns.str.replace(term,'_')


test = pd.read_csv(r'C:/Users/Jonathan/Desktop/Thesis/unified2212_cleaned.csv')
test1 = pd.read_csv(r'C:/Users/Jonathan/Desktop/Thesis/unified2212_cleaned_reindex.csv')





def sentiment(dfs):
    startTime = datetime.now()
    dfs['sentiment_polarity'] = '' 
    for row in range(len(dfs)):
        text = str(dfs['unified_text'].get_value(row))
        sentiment = client.Sentiment({'text': text})
        dfs['sentiment_polarity'][row] = sentiment
        if row%60 is 0:
            sleep(60)
    print(datetime.now() - startTime)    
    
sentiment(test1)

test2['Unnamed: 0'] = test2[['Unnamed: 0'] - 2

import googletrans      
translator = googletrans.Translator()
trans = translator.translate('irritating gile babi pe train fault beh bus stop cramp nak mampos', dest= 'en', src= 'ms')     
trans = translator.translate('irritating gile babi pe train fault beh bus stop cramp nak mampos')      
 
print(trans.origin)

transtest1 = test2.copy()
ph_list= [96,135, 215, 276, 291, 383, 440, 449, 518 ]
ph_set = set(ph_list)
jp_list= 71
ma_list = [119,152, 164, 168, 203, 204, 221, 232, 245, 271, 294, 296, 298, 305, 315, 316, 322, 323, 330, 362, 386, 395, 400, 404, 416, 419, 420, 437, 429, 444, 447, 450, 451, 477, 492, 493, 496, 497, 505, 530, 539, 554, 568, 588, 628, 668 ] 

def translate_unified_text(df):
    for row in range(len(df)):
        translator = googletrans.Translator()
#        if df['Unnamed: 0'].get_value(row) in ph_list:
        text = df['unified_text'].get_value(row)
        text = emoji.demojize(text)
        trans = translator.translate(text, dest= 'en')
        df['unified_text'][row]= trans.text
#        elif row in jp_list:
#            trans = translator.translate(text, dest= 'en', src= 'ja')
#        elif row in ma_list:
#            trans = translator.translate(text, dest= 'en', src= 'ms')
#        else:
#            pass
#
#def translate_unified_text1(df):            
#    for index, row in df.iterrows():
#        translator = googletrans.Translator()
#        df
#        df['unified_text'] = df['unified_text'].map(lambda x: translator.translate(x, dest="en").text)
#
#for i in ph_list:
#    if i == df['Unnamed: 0'].get_value(row) 
    

def translate_unified_text2(df):
    for row in range(len(df)):
        translator = googletrans.Translator()
        if df['Unnamed: 0'].get_value(row) in ph_set:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest='en' , src='tl')
            df['unified_text'][row]= trans.text
        elif df['Unnamed: 0'].get_value(row) in jp_list:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest= 'en', src= 'ja')
            df['unified_text'][row]= trans.text
        elif df['Unnamed: 0'].get_value(row) in ma_list:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest= 'en', src= 'ms')
            df['unified_text'][row]= trans.text
        else:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            df['unified_text'][row]= text1
            
translate_unified_text2(transtest1)
transtest.to_csv('C:/Users/Jonathan/Desktop/Thesis/transtest2412.csv')
transtest1.to_csv('C:/Users/Jonathan/Desktop/Thesis/transtest24121.csv') 

-------------

#abbr_dict= {"wtf": "what the fuck",
#  "wth": "what the hell",
#  "omg": 'oh my god', 
#  'Focking ell' : 'fucking hell' ,
#  'Focking ': 'fucking ', 
#  ' ell': ' hell ',
#   'FML': 'fuck my life',
#   ' tf': 'the fuck',
#   'fak': 'fuck',
#   'fkn': 'fucking',
#   'fck': 'fuck',
#   'BRUH': 'brother',
#   'fking': 'fucking',
#   'wts':'what the shit',
#   'jeez':'jesus',
#   'WTFFFFFFF': 'what the fuck',
#   'Daayyuumm': 'damn',
#   'stfu': 'shut the fuck up',
#   'Mygawwdd': 'my god', 
#   'Daayyuummm': 'damn ',
#   "fk" : "fuck",
#   "Fk" : "fuck",
#   'fxck': 'fuck',
#   'lmaO': 'laughing my ass off',
#   'lol': 'laughing out loud',
#}

abbr_dict1= {"wtf": "what the fuck",
  "wth": "what the hell",
  "omg": 'oh my god', 
  'focking ell' : 'fucking hell' ,
   'fml': 'fuck my life',
   ' tf': 'the fuck',
   'fak': 'fuck',
   'fkn': 'fucking',
   'fck': 'fuck',
   'bruh': 'brother',
   'fking': 'fucking',
   'wts':'what the shit',
   'jeez':'jesus',
   'wtfffffff': 'what the fuck',
   'daayyuumm': 'damn',
   'stfu': 'shut the fuck up',
   'mygawwdd': 'my god', 
   'daayyuummm': 'damn ',
   "fk" : "fuck",
   "Fk" : "fuck",
   'fxck': 'fuck',
   'lmao': 'laughing my ass off',
   'lol': 'laughing out loud',
   'pls': 'please',
}

vernac_dict= {"cb": "cunt",
  'ccb': 'rotten cunt',            
  "kena": "got",
  'knn': 'fuck your mother',
  'alamak': 'oh my god',
  'gile': 'mad',
  'babi': 'pig', 
  'merepek': 'gibberish',
  'wayang': 'fake',
  'nabei':'your father',
  'liao':'already',
  'walao':'oh my god',
  'kimak': "your mother's cunt",
  'kimek': "your mother's cunt",
  'kmk': "your mother's cunt",
  'aiya': 'oh dear',
  'aiyo': 'oh no',
  'bapak kau': 'your father',
  'bodoh': 'idiot',
  'sial': 'damn it',
  'lanjiao': 'dick',
  'sian': 'bummer',
  'sienz': 'bummer',
  'swee':'great',
  'ya allah': 'oh god',
  }

comb_dict= abbr_dict1.copy()
comb_dict.update(vernac_dict)


dict_test = 'wtf wth omg'

#def trans_abbr(i):
#    dict_split= i.split()
#    for a in dict_split:
#        for x, y in abbr_dict.items():
#            if a==x:
#                i[a]=y
#            else:
#                pass


def trans_abbr1(i):
    for x, y in abbr_dict.items():
       i = re.sub(x,y,i)
        
trans_abbr1(dict_test)

#def strTr( text, dic ):
#    """ Replaces keys of dic with values of dic in text. 2005-02 by Emanuel Rumpf (http://code.activestate.com/recipes/81330-single-pass-multiple-replace/)"""
#    pat = "(%s)" % "|".join( map(re.escape, dic.keys())  )
#    return re.sub( pat, lambda m:dic[m.group()], text )


def strTr1( text, dic ):
    """ Replaces keys of dic with values of dic in text. 2005-02 by Emanuel Rumpf (http://code.activestate.com/recipes/81330-single-pass-multiple-replace/)"""
    pat = "(%s)" % "|".join( map(re.escape, str(dic.keys()).lower())  )
    return re.sub( pat.lower(), lambda m:dic[m.group()], text )



abbrtest = transtest1.copy()
for row in range(len(abbrtest)):
    text = str(abbrtest['unified_text'].get_value(row))
    abbrtest['unified_text'][row]= trans_abbr1(text)

for row in range(len(abbrtest)):
        text = str(abbrtest['unified_text'].get_value(row))
        abbrtest['unified_text'][row]=  strTr( text, abbr_dict )
    
 --------                       
import vaderSentiment
from vaderSentiment import sentiment as vaderSentiment
sentence = 'train stuck wts (@ Admiralty MRT Station (NS10) - @smrt_singapore in Singapore) https://t.co/ztGvf8wL2Q' 
sentiment = vaderSentiment()

#from googletrans import Translator
#translator = Translator()
#translator.translate('안녕하세요.')
#
-----
class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        key = key.lower()
        dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        key = key.lower()
        return dict.__getitem__(self, key)
d ={"wtf": "what the fuck",
  "wth": "what the hell",
  "omg": 'oh my god', 
  'focking': 'fucking',
  'ell': 'hell',
   'fml': 'fuck my life',
   'tf': 'the fuck',
   'fak': 'fuck',
   'fkn': 'fucking',
   'fck': 'fuck',
   'bruh': 'brother',
   'fking': 'fucking',
   'wts':'what the shit',
   'jeez':'jesus',
   'wtfffffff': 'what the fuck',
   'daayyuumm': 'damn',
   'stfu': 'shut the fuck up',
   'mygawwdd': 'my god', 
   'daayyuummm': 'damn ',
   "fk" : "fuck",
#   "Fk" : "fuck",
}
d = CaseInsensitiveDict(d)
d["Python"] = "Easy"
print d["PYTHON"]
print d["Python"]

for row in range(len(abbrtest)):
        text = str(abbrtest['unified_text'].get_value(row))
        abbrtest['unified_text'][row]=  strTr( text, d )

# ********************************************
# class     caselessDict
# purpose   emulate a normal Python dictionary
#           but with keys which can accept the
#           lower() method (typically strings).
#           Accesses to the dictionary are
#           case-insensitive but keys returned
#           from the dictionary are always in
#           the original case.
# ********************************************

class caselessDict:
    def __init__(self,inDict=None):
        """Constructor: takes conventional dictionary
           as input (or nothing)"""
        self.dict = {}
        if inDict != None:
            for key in inDict:
                k = key.lower()
                self.dict[k] = (key, inDict[key])
        self.keyList = self.dict.keys()
        return

    def __iter__(self):
        self.iterPosition = 0
        return(self)

    def next(self):
        if self.iterPosition >= len(self.keyList):
            raise StopIteration
        x = self.dict[self.keyList[self.iterPosition]][0]
        self.iterPosition += 1
        return x

    def __getitem__(self, key):
        k = key.lower()
        return self.dict[k][1]

    def __setitem__(self, key, value):
        k = key.lower()
        self.dict[k] = (key, value)
        self.keyList = self.dict.keys()

    def has_key(self, key):
        k = key.lower()
        return k in self.keyList

    def __len__(self):
        return len(self.dict)

    def keys(self):
        return [v[0] for v in self.dict.values()]

    def values(self):
        return [v[1] for v in self.dict.values()]

    def items(self):
        return self.dict.values()

    def __contains__(self, item):
        return self.dict.has_key(item.lower())

    def __repr__(self):
        items = ", ".join([("%r: %r" % (k,v)) for k,v in self.items()])
        return "{%s}" % items

    def __str__(self):
        return repr(self)

e = abbr_dict.copy()
e = caselessDict(e)

for row in range(len(abbrtest)):
        text = str(abbrtest['unified_text'].get_value(row))
        abbrtest['unified_text'][row]=  strTr( text, e )

---------
import difflib
from collections import OrderedDict

class FuzzyDict(OrderedDict):

    """Provides a dictionary that performs fuzzy lookup"""

    def __init__(self, items = None, cutoff = .6):
        """
        Construct a new FuzzyDict instance

        items is an dictionary to copy items from (optional)
        cutoff is the match ratio below which mathes should not be considered
        cutoff needs to be a float between 0 and 1 (where zero is no match
        and 1 is a perfect match).
        """
        super(FuzzyDict, self).__init__()

        # short wrapper around some super (OrderedDict) methods
        self._dict_contains = lambda key: \
            super(FuzzyDict, self).__contains__(key)

        self._dict_getitem = lambda key: \
            super(FuzzyDict, self).__getitem__(key)

        self.cutoff =  cutoff
        if items:
            self.update(items)

    def _search(self, lookfor, stop_on_first = False):
        """
        Returns the value whose key best matches lookfor

        if stop_on_first is True then the method returns as soon
        as it finds the first item.
        """
        # if the item is in the dictionary then just return it
        if self._dict_contains(lookfor):
            return True, lookfor, self._dict_getitem(lookfor), 1

        # set up the fuzzy matching tool
        ratio_calc = difflib.SequenceMatcher()
        ratio_calc.set_seq1(lookfor)

        # test each key in the dictionary
        best_ratio = 0
        best_match = None
        best_key = None
        for key in self:

            # if the current key is not a string
            # then we just skip it
            try:
                # set up the SequenceMatcher with other text
                ratio_calc.set_seq2(key)
            except TypeError:
                continue

            # we get an error here if the item to look for is not a
            # string - if it cannot be fuzzy matched and we are here
            # this it is defintely not in the dictionary
            try:
                # calculate the match value
                ratio = ratio_calc.ratio()
            except TypeError:
                break

            # if this is the best ratio so far - save it and the value
            if ratio > best_ratio:
                best_ratio = ratio
                best_key = key
                best_match = self._dict_getitem(key)

            if stop_on_first and ratio >= self.cutoff:
                break

        return (
            best_ratio >= self.cutoff,
            best_key,
            best_match,
            best_ratio)

    def __contains__(self, item):
        """Overides OrderedDict __contains__ to use fuzzy matching"""
        if self._search(item, True)[0]:
            return True
        else:
            return False

    def __getitem__(self, lookfor):
        """Overides OrderedDict __getitem__ to use fuzzy matching"""
        matched, key, item, ratio = self._search(lookfor)

        if not matched:
            raise KeyError("'{0}'. closest match: '{1}' with ratio {2}".\
                format(str(lookfor), str(key), ratio))

        return item

f= abbr_dict1.copy()
f = FuzzyDict(f)
abbrtest1 = transtest1.copy()

for row in range(len(abbrtest1)):
        text = str(abbrtest1['unified_text'].get_value(row))
        abbrtest1['unified_text'][row]=  strTr1( text, f )

------

#Jeff Hykin (http://code.activestate.com/recipes/81330-single-pass-multiple-replace/)
def SimulSub ( dict_ , string_ ):
  if len(dict_) == 0:
    return string_
  def repl_(regex_):
    match_ = regex_.group()
    for x in (sorted(dict_.keys(), key=lambda x: len(x))):
      # the below line could cause problems for lookahead / lookbehind / beginning / end regex 
      if (re.search( x.lower() , match_ ) != None): return dict_[ x ] 
    print( "error with SimulSub")
    print( "check lookahead/lookbehind/beginning/end regex")
    return match_
  return re.sub( "(" + "|".join(sorted(dict_.keys(), key=lambda x: len(x),reverse=True)) + ")"  , repl_ , string_ )

for row in range(len(abbrtest1)):
        text = str(abbrtest1['unified_text'].get_value(row)).lower()
        abbrtest1['unified_text'][row]=  SimulSub( f,text  )

for row in range(len(abbrtest1)):
        text = str(abbrtest1['unified_text'].get_value(row)).lower()
        abbrtest1['unified_text'][row]=  SimulSub( vernac_dict,text  )
--------
#compile

test = pd.read_csv(r'C:/Users/Jonathan/Desktop/Thesis/unified2212_cleaned.csv')
test1 = pd.read_csv(r'C:/Users/Jonathan/Desktop/Thesis/unified2212_cleaned_reindex.csv')

def sentiment(dfs):
    startTime = datetime.now()
    dfs['sentiment_polarity'] = '' 
    for row in range(len(dfs)):
        text = str(dfs['unified_text'].get_value(row))
        sentiment = client.Sentiment({'text': text})
        dfs['sentiment_polarity'][row] = sentiment
        if row%60 is 0:
            sleep(60)
    print(datetime.now() - startTime)    
    
test2['Unnamed: 0'] = test2[['Unnamed: 0'] - 2

import googletrans      
transtest1 = test2.copy()
ph_list= [96,135, 215, 276, 291, 383, 440, 449, 518 ]
ph_set = set(ph_list)
jp_list= 71
ma_list = [119,152, 164, 168, 203, 204, 221, 232, 245, 271, 294, 296, 298, 305, 315, 316, 322, 323, 330, 362, 386, 395, 400, 404, 416, 419, 420, 437, 429, 444, 447, 450, 451, 477, 492, 493, 496, 497, 505, 530, 539, 554, 568, 588, 628, 668 ] 

def translate_unified_text2(df):
    for row in range(len(df)):
        translator = googletrans.Translator()
        if df['Unnamed: 0'].get_value(row) in ph_set:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest='en' , src='tl')
            df['unified_text'][row]= trans.text
        elif df['Unnamed: 0'].get_value(row) in jp_list:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest= 'en', src= 'ja')
            df['unified_text'][row]= trans.text
        elif df['Unnamed: 0'].get_value(row) in ma_list:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            trans = translator.translate(text1, dest= 'en', src= 'ms')
            df['unified_text'][row]= trans.text
        else:
            text = str(df['unified_text'].get_value(row))
            text1 = emoji.demojize(text)
            df['unified_text'][row]= text1
            
translate_unified_text2(transtest1)
transtest.to_csv('C:/Users/Jonathan/Desktop/Thesis/transtest2412.csv')
transtest1.to_csv('C:/Users/Jonathan/Desktop/Thesis/transtest24121.csv') 




abbr_dict1= {"wtf": "what the fuck",
  "wth": "what the hell",
  "omg": 'oh my god', 
  'focking ell' : 'fucking hell' ,
   'fml': 'fuck my life',
   ' tf': 'the fuck',
   'fak': 'fuck',
   'fkn': 'fucking',
   'fck': 'fuck',
   'bruh': 'brother',
   'fking': 'fucking',
   'wts':'what the shit',
   'jeez':'jesus',
   'wtfffffff': 'what the fuck',
   'daayyuumm': 'damn',
   'stfu': 'shut the fuck up',
   'mygawwdd': 'my god', 
   'daayyuummm': 'damn ',
   "fk" : "fuck",
   "Fk" : "fuck",
   'fxck': 'fuck',
   'lmao': 'laughing my ass off',
   'lol': 'laughing out loud',
   'pls': 'please',
}

vernac_dict= {"cb": "cunt",
  'ccb': 'rotten cunt',            
  "kena": "got",
  'knn': 'fuck your mother',
  'alamak': 'oh my god',
  'gile': 'mad',
  'babi': 'pig', 
  'merepek': 'gibberish',
  'wayang': 'fake',
  'nabei':'your father',
  'liao':'already',
  'walao':'oh my god',
  'kimak': "your mother's cunt",
  'kimek': "your mother's cunt",
  'kmk': "your mother's cunt",
  'aiya': 'oh dear',
  'aiyo': 'oh no',
  'bapak kau': 'your father',
  'bodoh': 'idiot',
  'sial': 'damn it ',
  'lanjiao': 'dick',
  'sian': 'bummer',
  'sienz': 'bummer',
  'swee':'great',
  'ya allah': 'oh god',
  'haihh': 'sigh'
  }

comb_dict= abbr_dict1.copy()
comb_dict.update(vernac_dict)

abbrtest1 = transtest1.copy()

def SimulSub ( dict_ , string_ ):
    #Jeff Hykin (http://code.activestate.com/recipes/81330-single-pass-multiple-replace/)
  if len(dict_) == 0:
    return string_
  def repl_(regex_):
    match_ = regex_.group()
    for x in (sorted(dict_.keys(), key=lambda x: len(x))):
      # the below line could cause problems for lookahead / lookbehind / beginning / end regex 
      if (re.search( x.lower() , match_ ) != None): return dict_[ x ] 
    print( "error with SimulSub")
    print( "check lookahead/lookbehind/beginning/end regex")
    return match_
  return re.sub( "(" + "|".join(sorted(dict_.keys(), key=lambda x: len(x),reverse=True)) + ")"  , repl_ , string_ )

for row in range(len(abbrtest1)):
        text = str(abbrtest1['unified_text'].get_value(row)).lower()
        abbrtest1['unified_text'][row]=  SimulSub( comb_dict,text  )

abbrtest1.to_csv('C:/Users/Jonathan/Desktop/Thesis/abbrtest1.csv')

re_sentiment = abbrtest1[['Unnamed: 0','unified_text']].copy()
re_sentiment['re_sentiment'] = ''
def sentiment(dfs):
    startTime = datetime.now()
    dfs['re_sentiment'] = '' 
    for row in range(len(dfs)):
        text = str(dfs['unified_text'].get_value(row))
        sentiment = client.Sentiment({'text': text})
        dfs['re_sentiment'][row] = sentiment
        if row%60 is 0:
            sleep(60)
    print(datetime.now() - startTime)    
    
sentiment(re_sentiment)
re_sentiment1 = pd.concat([re_sentiment.drop(['re_sentiment'], axis=1), re_sentiment['re_sentiment'].apply(pd.Series)], axis=1)

re_sentiment1.columns = [str(col) + '_re' for col in re_sentiment1.columns]

comparison = abbrtest1[['id', 'Unnamed: 0','unified_text', 'polarity', 'subjectivity',
       'polarity_confidence', 'subjectivity_confidence']].merge(re_sentiment1[['Unnamed: 0_re','polarity_re', 'subjectivity_re',
       'text_re', 'polarity_confidence_re', 'subjectivity_confidence_re']], left_on=['Unnamed: 0'], right_on=['Unnamed: 0_re'] )

comparison1 = test2[['id', 'Unnamed: 0','unified_text', 'polarity', 'subjectivity',
       'polarity_confidence', 'subjectivity_confidence']].merge(re_sentiment1[['Unnamed: 0_re','polarity_re', 'subjectivity_re',
       'text_re', 'polarity_confidence_re', 'subjectivity_confidence_re']], left_on=['Unnamed: 0'], right_on=['Unnamed: 0_re'] )

comparison1['polarity_changed']= ''
for  row in range(len(comparison1)):
    if comparison1['polarity'].get_value(row) == comparison1['polarity_re'].get_value(row):
        comparison1['polarity_changed'][row]= 0
    else:
        comparison1['polarity_changed'][row]= 1

comparison1['polarity_conf_changed']= ''
for  row in range(len(comparison1)):
    a=''
    b=''
   
    if comparison1['polarity_re'].get_value(row) == 'positive':
        a= comparison1['polarity_confidence_re'].get_value(row) + 2
    elif comparison1['polarity_re'].get_value(row) == 'neutral':
        a= comparison1['polarity_confidence_re'].get_value(row) + 1
    else:
        a= comparison1['polarity_confidence_re'].get_value(row)
    pass
    if comparison1['polarity'].get_value(row) == 'positive':
        b= comparison1['polarity_confidence'].get_value(row) + 2
    elif comparison1['polarity_re'].get_value(row) == 'neutral':
        b= comparison1['polarity_confidence'].get_value(row) + 1
    else:
        b= comparison1['polarity_confidence'].get_value(row)
    comparison1['polarity_conf_changed'][row]= a-b



