import numpy as np
import pandas as pd
import joblib
import os
import re
import shap
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score, roc_curve
from sklearn.metrics import (confusion_matrix, precision_recall_curve, fbeta_score,
                            classification_report, brier_score_loss, precision_score)
from sklearn.calibration import calibration_curve
from sklearn.preprocessing import binarize, normalize, minmax_scale, MinMaxScaler, StandardScaler, scale
from sklearn.linear_model import LogisticRegression

from tqdm import tqdm

from catboost import CatBoostClassifier, Pool
import json
from sklearn.model_selection import train_test_split, StratifiedKFold, StratifiedShuffleSplit


def get_f1(true, proba, threshold):
    '''
    Prepare probabilities & calculate f1 score
    DEPENDENCIES:
        *  from sklearn.metrics import f1_score
        *  from sklearn.preprocessing import binarize
    '''
    pred_class = binarize(proba.reshape(-1,1), threshold=threshold).flatten()
    return f1_score(true, pred_class)    

def get_best_threshold(y_true, y_proba):
    '''
    Calculate thresholds & choose the one that yields the best f1-score
    DEPENDENCIES:
        *  from sklearn.metrics import precision_recall_curve
    '''
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    max_f1 = 0
    for r, p, t in zip(recall, precision, thresholds):
        if p + r == 0: continue
        if (2*p*r)/(p + r) > max_f1:
            max_f1 = (2*p*r)/(p + r)
            max_f1_threshold = t
    return max_f1_threshold

def show_venn_diagram(val, threshold, column):
    '''
    Build Venn diagram between true positive & predicted positive 
    ARGS:
        * val - data (with label & pred columns)
        * threshold - for counting positives
        * column - prediction column
    DEPENDENCIES:
        *  from sklearn.metrics import precision_recall_curve   
    '''
    if threshold == 'best':
        threshold = get_best_threshold(val.true, val[column])
    print('F1: {:.5f}'.format(get_f1(val.true, val[column], threshold)))
    plt.figure(figsize=(4,4))
    true_inns = val[val.true == 1].index
    pred_inns = val[val[column] > threshold].index
    venn2([set(true_inns), set(pred_inns)], set_labels=('true','preds@{:.2f}'.format(threshold)))

def calibration_plot(df, target_col='target', score_col='score', bins=10, quantile=False):
    '''
    Build calibration barplot
    ARGS:
        * df - data (with label & pred columns)
        * target_col - column with target labels
        * score_col - column with predicted scores
        * bins - number of bins
        * quantile - binning strategy:
                            ** False - bins with equal width
                            ** True  - bins with equal height
    DEPENDENCIES:
        *  import pandas as pd
        *  import numpy as np
        *  import matplotlib.pyplot as plt
    '''
    dfc = df.copy()
    dfc['neg_target'] = dfc[target_col] == 0
    if quantile:
        dfc['bin'] = pd.qcut(dfc[score_col], q=bins, retbins=False, labels=None, precision=2)
    else:
        borders = np.linspace(0, 1, bins+1)
        dfc['bin'] = pd.cut(dfc[score_col], bins=borders, retbins=False, labels=None, precision=2)
    dfc['pos_frac'] = dfc.groupby('bin')[target_col].transform('mean')
    dfc['pos_count'] = dfc.groupby('bin')[target_col].count()
    dfc['neg_count'] = dfc.groupby('bin')[target_col].count()
    dfc['neg_frac'] = dfc.groupby('bin')['neg_target'].transform('mean') 
    dfc['avg_score'] = dfc.groupby('bin')[score_col].transform('mean')
    dfc['sample_frac'] = dfc.groupby('bin')[score_col].transform('count') / dfc.shape[0]
    qbin_df = dfc.reset_index(drop=True).drop([target_col, score_col], 1).drop_duplicates(subset=['bin']).set_index('bin').sort_index()
    plt.bar(height=qbin_df.neg_frac, x=range(bins), label='доля отрицательного класса', alpha=0.6)
    plt.bar(height=qbin_df.pos_frac, x=range(bins), label='доля положительного класса', alpha=0.6, bottom=qbin_df.neg_frac)
    plt.bar(height=qbin_df.sample_frac, x=range(bins), label='доля от общего объема', width=0.15, color='gray')
    plt.xticks(ticks=range(bins), labels=qbin_df.index, rotation=90)
    plt.legend(loc='best', bbox_to_anchor=(1, 0.5))
    if quantile:
        plt.title('Распределение по бинам (равная высота)')
    else:
        plt.title('Распределение по бинам (равные интервалы)')
    return qbin_df

def print_diagrams(y_test, y_test_proba):
    '''
    Print scores distribution by target (pos/neg),
          calibration curve,
          precision-recall curve  
    DEPENDENCIES:
        *  import seaborn as sns
        *  import matplotlib.pyplot as plt
        *  from sklearn.metrics import precision_recall_curve
        *  from sklearn.calibration import calibration_curve
    '''
    fig, ax = plt.subplots(1, 3, figsize=(13, 4))

    sns.distplot(y_test_proba[y_test == 0], label='0', norm_hist=True, hist=False, ax=ax[0])
    sns.distplot(y_test_proba[y_test == 1], label='1', norm_hist=True, hist=False, ax=ax[0])
    ax[0].legend(loc='best', title='таргет')
    ax[0].set_title('Распределение таргета по скорам')

    x, y = calibration_curve(y_test, y_test_proba, n_bins=10, normalize=False)
    ax[1].plot(y, x,'o-',label='val')
    ax[1].plot([0, 1], [0, 1], 'k:')
    ax[1].set_xlabel('средний скор')
    ax[1].set_ylabel('доля положительного класса')
    ax[1].set_title('Кривая калибровки')

    precision, recall, thresholds = precision_recall_curve(y_test, y_test_proba)
    thresholds = np.asarray([0.0] + thresholds.tolist())
    ax[2].plot(thresholds, precision, label='precision')
    ax[2].plot(thresholds, recall, label='recall')
    ax[2].set_xlabel('порог')
    ax[2].set_ylabel('метрика')
    ax[2].set_title('Precision-Recall')
    ax[2].legend(loc='best')
