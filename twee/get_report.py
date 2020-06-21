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

stops = [str(x) for x in stopwords.words('english')]


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

def consolidate_grams(grams_dir):
    file_counts = []

    file_list = glob.glob(join(grams_dir, '*.csv'))
    tmp = []
    for file_ in file_list:
        print(file_)
        tmp.append(pd.read_csv(file_, encoding='utf-8'))
    tmp = pd.concat(tmp)
    tmp = tmp.groupby(['grams', 'label', 'month'])[['num_label', 'num_notlabel']].sum().reset_index()
    tmp.columns = ['grams', 'label', 'month', 'num_label', 'num_notlabel']
    tmp = tmp[tmp['num_label'] > 1]
    file_counts.append(tmp)

    overall_counts = pd.concat(file_counts)

    overall_counts = overall_counts.groupby(['grams', 'label', 'month'])[['num_label', 'num_notlabel']].sum().reset_index()
    overall_counts.columns = ['grams', 'label', 'month', 'num_label', 'num_notlabel']

    counts_cl = overall_counts[overall_counts['num_label'] > 50]

    summary = overall_counts.groupby(['label', 'month'])[['num_label', 'num_notlabel']].sum().reset_index()
    summary.columns = ['label', 'month', 'num_label_sum', 'num_notlabel_sum']

    return counts_cl, summary


def get_ratios(counts_column, summry):
    all_ratios = []

    for label in labels:

        summary_0 = summry[(summry['label'] == label) & (summry['month'] == 0)]['num_label_sum'].values[0]
        count_0 = counts_column[(counts_column['label'] == label) & (counts_column['month'] == 0)]

        for month in [1, 2, 3]:
            tmp_summary = summry[(summry['label'] == label) & (summry['month'] == month)]['num_label_sum'].values[0]
            tmp_counts = counts_column[(counts_column['label'] == label) & (counts_column['month'] == month)]

            ratio = pd.merge(tmp_counts[['grams', 'num_label']], count_0[['grams', 'num_label']], on='grams',
                             how='left')
            ratio['summary_month'] = tmp_summary
            ratio['summary_0'] = summary_0

            ratio['ratio'] = ratio['num_label_x'] * ratio['summary_0'] / ratio['summary_month'] / ratio['num_label_y']

            ratio['label'] = label
            ratio['month'] = month

            all_ratios.append(ratio)

    all_ratios = pd.concat(all_ratios)

    all_ratios.sort_values('ratio', ascending=False)

    all_ratios_cl = all_ratios[(all_ratios['ratio'] > 1) | pd.isnull(all_ratios['ratio'])].copy()

    all_ratios_cl['ratio'] = np.where(pd.isnull(all_ratios_cl['ratio']), all_ratios_cl['num_label_x'],
                                      all_ratios_cl['ratio'])


    # food_grams_counts = ratios_food.grams.value_counts().reset_index()
    # food_grams_counts.columns = ['grams', 'num_month']

    print(all_ratios.columns)
    print(all_ratios)

    return all_ratios



def pivot_grams(df):

    all_months_pv = pd.pivot_table(df, index='grams', columns='month',
                                        values='ratio').reset_index().sort_values(3, ascending=False)
    print(all_months_pv)

    return all_months_pv


def trend(x):
    delta_1_3 = x[3] - x[1]
    delta_1_2 = np.sign(x[2] - x[1])
    delta_2_3 = np.sign(x[3] - x[2])
    return delta_1_3 * delta_1_2 * delta_2_3


def apply_trend(df):
    df['trend'] = df.apply(lambda x: trend(x), axis=1)

    print(df)

    return df



def gram_selection(df1):
    tmp_df = df1['grams', 'ratio']

    tmp_df['ratio1'] = tmp_df['ratio_x'] / tmp_df['ratio_y']
    tmp_df['ratio2'] = tmp_df['ratio_x'] / tmp_df['ratio']

    tmp_df_a = tmp_df[pd.isnull(tmp_df['ratio_y']) & pd.isnull(tmp_df['ratio'])]
    tmp_df_b = tmp_df[(tmp_df['ratio1'] > 3) & pd.isnull(tmp_df['ratio'])]
    tmp_df_c = tmp_df[pd.isnull(tmp_df['ratio_y']) & (tmp_df['ratio2'] > 3)]
    tmp_df_d = tmp_df[(tmp_df['ratio1'] > 3) & (tmp_df['ratio2'] > 3)]

    tmp_df = pd.concat([tmp_df_a, tmp_df_b, tmp_df_c, tmp_df_d])
    tmp_df = tmp_df.sort_values('ratio_x', ascending=False)

    return tmp_df


if __name__ == '__main__':

    grams_files = 'grams_folder/'
    audience_dir = 'audiences/'

    savvy, ids_per_label, labels = get_audience_file(audience_dir, 'davide_savvy_fan_ids2.csv')

    count_cl, summary = consolidate_grams(grams_files)

    main_df = get_ratios(count_cl, summary)

    piv = pivot_grams(main_df)



