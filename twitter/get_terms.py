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


def get_tweet_ngrams(file_list):
    for file_ in file_list:
        print(file_)
        start = time.time()
        tweet_file_df = pd.read_csv(open(file_), encoding='utf-8', engine='c')

        tweet_file_df ['text_split'] = tweet_file_df['tweet_text'].apply(lambda x: ngram_pandas(x))

        out = pd.DataFrame(tweet_file_df['text_split'].tolist()).stack()

        print(out)

        # s_2 = s_2.reset_index()[[0, 'index']]  # col1 variable is currently labeled 0
        # s_2.columns = ['grams', 'index']
        # s_2.grams = s_2.grams.str.lower()
        #
        # s_2 = s_2[s_2['grams'].str.contains('[a-z]')]
        # s_2 = s_2[~s_2['grams'].isin(stops)]
        #
        # s_2 = pd.merge(df[['index', 'id', 'week', 'month']], s_2, on='index')
        # s_2 = pd.merge(s_2, savvy, on='id')
        #
        # print("    counting grams")


if __name__ == '__main__':

    FILES_DIR = 'account_tweets/'
    tweet_file_list = glob.glob(join(FILES_DIR, '*.csv'))

    get_tweet_ngrams(tweet_file_list)
