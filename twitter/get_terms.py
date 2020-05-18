import pandas as pd
import numpy as np
import re
import glob
import os
from os.path import join
import time
from datetime import datetime
import nltk
from nltk.corpus import stopwords
import argparse

stops = [str(x) for x in stopwords.words('english')]

def convert_to_sec(x):
    return time.mktime(time.strptime(x, "%Y-%m-%d %H:%M:%S"))


def convert_to_month(x):

    jan_5 = time.mktime(time.strptime('2020-01-05 00:00:00', "%Y-%m-%d %H:%M:%S"))
    feb_5 = time.mktime(time.strptime('2020-02-05 00:00:00', "%Y-%m-%d %H:%M:%S"))
    mar_5 = time.mktime(time.strptime('2020-03-05 00:00:00', "%Y-%m-%d %H:%M:%S"))

    if x < jan_5:
        month = 0
    elif x >= jan_5 and x < feb_5:
        month = 1
    elif x >= feb_5 and x < mar_5:
        month = 2
    elif x >= mar_5:
        month = 3
    return month


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


def get_audience_file(directory, filename):
    savvy = pd.read_csv(join(directory, filename), usecols=['twitter_id', 'label'])
    savvy.columns = ['id', 'label']
    savvy = savvy.drop_duplicates()

    labels = savvy.label.unique().tolist()

    savvy_ids_per_label = []

    for label in labels:
        label_ids = savvy[savvy['label'] == label]
        label_ids.columns = ['id', 'label_']
        label_not_ids = pd.merge(savvy, label_ids, on='id', how='left')
        label_not_ids = label_not_ids[pd.isnull(label_not_ids['label_'])].drop('label_', axis=1)
        label_not_ids['label'] = 'not{}'.format(label)
        label_not_ids = label_not_ids.drop_duplicates()
        label_ids.columns = ['id', 'label']
        savvy_ids_per_label.append(label_ids)
        savvy_ids_per_label.append(label_not_ids)
        # print(label, len(label_ids), len(label_not_ids), len(label_ids) + len(label_not_ids))

    savvy_ids_per_label = pd.concat(savvy_ids_per_label)
    return savvy, savvy_ids_per_label, labels



def get_tweet_files(directory):

    file_list = glob.glob(join(directory, '*.csv'))

    return file_list



def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='getting ngrams from tweets collected for audience')
    parser.add_argument('--audience_filename', help='twitter consumer_secret', type=str)

    return parser.parse_args()


if __name__ == '__main__':

    args = parse_args()
    FILES_DIR = 'account_tweets/'
    AUD_DIR = 'audiences/'
    week_in_sec = 86400 * 7
    min_date = time.mktime(time.strptime('2019-01-05 00:00:00', "%Y-%m-%d %H:%M:%S"))

    files = get_tweet_files(FILES_DIR)
    savvy, ids_per_label, labels = get_audience_file(AUD_DIR, args.audience_filename)

    # davide_savvy_fan_ids2.csv

    for file in files:

        print(file)
        start = time.time()

        df = pd.read_csv(file, encoding='utf-8', engine='c')
        df = df.reset_index()
        df = df[(pd.notnull(df['id'])) & (pd.notnull(df['created_at'])) & (pd.notnull(df['tweet_text']))]

        print("    estimating date buckets")
        df['date_new'] = df['created_at'].apply(lambda x: convert_to_sec(x))
        df = df[df['date_new'] >= min_date].copy()

        df['week'] = df['date_new'].apply(lambda x: np.int(np.ceil((x - jan_5) / week_in_sec)))
        df['week'] = np.where(df['week'] < 0, 0, df['week'])
        df['month'] = df['date_new'].apply(lambda x: convert_to_month(x))

        print("    finding grams")
        s_ = df[['index', 'tweet_text']].copy()
        s_['text_split'] = s_['tweet_text'].apply(lambda x: ngram_pandas(x))

        s_2 = pd.DataFrame(s_['text_split'].tolist(), index=s_['index']).stack()
        s_2 = s_2.reset_index()[[0, 'index']]  # col1 variable is currently labeled 0
        s_2.columns = ['grams', 'index']
        s_2.grams = s_2.grams.str.lower()

        s_2 = s_2[s_2['grams'].str.contains('[a-z]')]
        s_2 = s_2[~s_2['grams'].isin(stops)]

        s_2 = pd.merge(df[['index', 'id', 'week', 'month']], s_2, on='index')
        s_2 = pd.merge(s_2, savvy, on='id')

        print("    counting grams")

        file_counts = []
        file_summary = []

        for label in labels:
            s_2_label = s_2[s_2['label'] == label]
            label_not_ids = ids_per_label[ids_per_label['label'] == 'not{}'.format(label)][['id']]
            s_2_notlabel = pd.merge(s_2, label_not_ids, on='id')

            s_2_label_month = s_2_label.groupby(['grams', 'label', 'month'])['id'].count().reset_index()
            s_2_notlabel_month = s_2_notlabel.groupby(['grams', 'month'])['id'].count().reset_index()

            s_2_label_month.columns = ['grams', 'label', 'month', 'num_label']
            s_2_notlabel_month.columns = ['grams', 'month', 'num_notlabel']

            s_2_grp = pd.merge(s_2_label_month, s_2_notlabel_month, how='left')

            file_counts.append(s_2_grp)

        file_counts = pd.concat(file_counts)

        grams_out_folder = 'grams_folder'

        if not os.path.exists(grams_out_folder):
            os.makedirs(grams_out_folder)
        now = datetime.now()

        acc_id = re.findall('(_[\d]*_)', file)

        file_counts.to_csv(os.path.join(grams_out_folder, 'grams' + acc_id[0] + now.strftime("%Y-%m-%d") + '.csv'),
                           index=False, encoding='utf-8')

