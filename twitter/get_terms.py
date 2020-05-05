import pandas as pd
import numpy as np
import re
import glob
import os
from os.path import join
import csv
import time
import nltk
from nltk.corpus import stopwords
import time


stops = [str(x) for x in stopwords.words('english')]


# week_in_sec = 86400*7
# min_date = time.mktime(time.strptime('Sat Jan 05 00:00:00 +0000 2019', "%a %b %d %H:%M:%S +0000 %Y"))
# jan_5 = time.mktime(time.strptime('Sun Jan 05 00:00:00 +0000 2020', "%a %b %d %H:%M:%S +0000 %Y"))
# feb_5 = time.mktime(time.strptime('Wed Feb 05 00:00:00 +0000 2020', "%a %b %d %H:%M:%S +0000 %Y"))
# mar_5 = time.mktime(time.strptime('Thu Mar 05 00:00:00 +0000 2020', "%a %b %d %H:%M:%S +0000 %Y"))
#
#
#
# def convert_to_sec(x):
#     return time.mktime(time.strptime(x, "%a %b %d %H:%M:%S +0000 %Y"))
#
#
# def convert_to_month(x):
#     if x < jan_5:
#         month = 0
#     elif x >= jan_5 and x < feb_5:
#         month = 1
#     elif x >= feb_5 and x < mar_5:
#         month = 2
#     elif x >= mar_5:
#         month = 3
#     return month


def ngrams_func(s, n=2, i=0):
    while len(s[i:i+n]) == n:
        yield s[i:i+n]
        i += 1

def ngram_pandas(text):
    text_ = re.sub('|'.join([',', '\.', '\?', '\!', u'\2026']), ' ', text)
    text_ = re.sub(r'\s+', ' ', text_)
    unigram = [x[0] for x in list(ngrams_func(text_.split(), n=1))]
    bigram  = [' '.join(x) for x in list(ngrams_func(text_.split(), n=2))]
    trigram = [' '.join(x) for x in list(ngrams_func(text_.split(), n=3))]
    return unigram + bigram + trigram

SAVVY_DIR = '/nfs/share/davides_data/content_collection/'
FILES_DIR = 'account_tweets'


savvy = '/audiences/davide_savvy_fan_ids.csv'


file_list = glob.glob(join(FILES_DIR, '*.csv'))

print(file_list)

for file_ in file_list:
    print(file_)
    start = time.time()

    df = pd.read_csv(open(file_), encoding='utf-8', engine='c')
    df = df.reset_index()
    print('    len of null ', len(df[pd.isnull(df['id']) | pd.isnull(df['created_at']) | pd.isnull(df['tweet_text'])]))
    df = df[pd.notnull(df['id']) & pd.notnull(df['created_at']) & pd.notnull(df['tweet_text'])]


    # print("    estimating date buckets")
    # df['date_new'] = df['created_at'].apply(lambda x: convert_to_sec(x))
    # df = df[df['date_new'] >= min_date].copy()
    #
    # df['week'] = df['date_new'].apply(lambda x: np.int(np.ceil((x - jan_5) / week_in_sec)))
    # df['week'] = np.where(df['week'] < 0, 0, df['week'])
    # df['month'] = df['date_new'].apply(lambda x: convert_to_month(x))

    print("    finding grams")
    s_ = df[['index', 'text']].copy()
    s_['text_split'] = s_['text'].apply(lambda x: ngram_pandas(x))

    s_2 = pd.DataFrame(s_['text_split'].tolist(), index=s_['index']).stack()
    s_2 = s_2.reset_index()[[0, 'index']]  # col1 variable is currently labeled 0
    s_2.columns = ['grams', 'index']
    s_2.grams = s_2.grams.str.lower()

    s_2 = s_2[s_2['grams'].str.contains('[a-z]')]
    s_2 = s_2[~s_2['grams'].isin(stops)]

    s_2 = pd.merge(df[['index', 'id', 'week', 'month']], s_2, on='index')
    s_2 = pd.merge(s_2, savvy, on='id')

    print("    counting grams")


