import os, getpass, re, subprocess, logging, sys
import numpy as np
import pandas as pd
import datetime as dt
import calendar as cal


def query_yes_no(question, default='yes'):
    valid = {'yes': True, 'y': True, 'ye': True, 'no': False, 'n': False}
    if default is None:
        prompt = " [y/n]"
    elif default == 'yes':
        prompt = " [Y/n]"
    elif default == 'no':
        prompt = " [y/N]"
    else:
        raise ValueError('Invalid default answer: {}'.format(default))

    while True:
        print(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' ('y' or 'n').\n")


def export_to_csv(df, file_name, compress=False, win=False):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    pdf = df.toPandas()
    param = {}
    if win:
        param = dict(sep=';', decimal=',', encoding='cp1251')
    if compress:
        param['compression'] = 'gzip'
    pdf.to_csv(file_name, index=False, **param)
    print('File saved:', file_name, pdf.shape)


def get_logger(level='INFO', filename='stdout'):
    if level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        raise ValueError('Log level name not exists: {}'.format(level))

    levels = {
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }

    if filename != 'stdout':
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        filename = '{}/../{}'.format(cur_dir, filename)
        filename_dir =  os.path.dirname(os.path.abspath(filename))
        os.makedirs(filename_dir, exist_ok=True)
        param = {'filename': filename}
    else:
        param = {'stream': sys.stdout}

    logging.basicConfig(format='%(levelname)-8s %(filename)s [%(asctime)s] %(message)s',
                        level=levels[level],
                        **param)
    logger = logging.getLogger(__name__)
    return logger


# Принимает дату в ISO-формате, возвращает дату в ISO-формате
def add_month(date, months):
    d_date = dt.datetime.strptime(date, '%Y-%m-%d')
    month = d_date.month + months
    year = d_date.year
    is_last_day = d_date.day == cal.monthrange(d_date.year, d_date.month)[1]
    while not (1 <= month <= 12):
        if month > 12:
            month = month - 12
            year += 1
        elif month < 1:
            month = 12 + month
            year -= 1
    if is_last_day:
        day = cal.monthrange(year, month)[1]
    else:
        day = min(d_date.day, cal.monthrange(year, month)[1])
    return dt.datetime(year, month, day).strftime('%Y-%m-%d')


def get_pass(td=False):
    file_name = 'test.txt' if td else 'testi.txt'
    curdir = os.getcwd()
    pss = None
    while curdir:
        file = os.path.join(curdir, file_name)
        if os.path.isfile(file):
            pss = open(file).read().strip()
            return pss
        curdir = re.sub(r'/[^/]*$', '', curdir)
    pss = getpass.getpass('Пароль от ИСКРЫ>>')
    w = open(os.path.join('..', file_name), 'w')
    print(pss, file=w)
    w.close()
    return pss


class SparkUtil():
    def __init__(self, local_path=None, hdfs_path=None, hive_path=None,
                 local_db=None, sqlContext=None):
        self.local_path = local_path
        self.hdfs_path = hdfs_path
        self.hive_path = hive_path
        self.local_db = local_db
        self.sqlContext = sqlContext


    def save_table(self, df, name):
        # self.drop_hive_table(name)
        df.registerTempTable(name)
        self.sqlContext.sql('DROP TABLE IF EXISTS {db}.{tab}'.format(db=self.local_db, tab=name))
        self.sqlContext.sql('CREATE TABLE {db}.{tab} SELECT * FROM {tab}'.format(db=self.local_db, tab=name))


    def drop_hive_table(self, table):
        hdfs_path = os.path.join(self.hive_path, table)
        proc = subprocess.Popen(['hdfs', 'dfs', '-chmod', '-R', '777', hdfs_path])
        proc.communicate()
        proc = subprocess.Popen(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', hdfs_path])
        proc.communicate()


    def load_as_csv(self, dst_df, file_name, save_local=True):
        local_path = os.path.join(self.local_path, file_name)
        hdfs_path = os.path.join(self.hdfs_path, file_name)

        dst_df \
            .coalesce(1).write \
            .format('com.databricks.spark.csv') \
            .mode('overwrite') \
            .option('sep', ',') \
            .option('header', 'true') \
            .save(hdfs_path)

        if save_local:
            proc = subprocess.Popen(['hdfs', 'dfs', '-getmerge', hdfs_path, local_path])
            proc.communicate()
            print('Import to file:', local_path)
        else:
            print('Import to HDFS:', hdfs_path)


def reduce_mem_usage(props):
    start_mem_usg = props.memory_usage().sum() / 1024**2
    print("Memory usage: {:.1f} Mb".format(start_mem_usg))
    NAlist = []

    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings

            # Print current column type
            print("Column:", col)
            print("dtype before:", props[col].dtype)

            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()

            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all() and not col.startswith('embed_'):
                NAlist.append(col)
                props[col].fillna(mn-1, inplace=True)
            elif col.startswith('embed_'):
                props[col].fillna(0, inplace=True)

            # test if column can be converted to an integer
            asint = props[col].fillna(0).astype(np.int64)
            result = (props[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True

            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        props[col] = props[col].astype(np.uint8)
                    elif mx < 65535:
                        props[col] = props[col].astype(np.uint16)
                    elif mx < 4294967295:
                        props[col] = props[col].astype(np.uint32)
                    else:
                        props[col] = props[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        props[col] = props[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        props[col] = props[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        props[col] = props[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        props[col] = props[col].astype(np.int64)

            # Make float datatypes 32 bit
            else:
                props[col] = props[col].astype(np.float32)

            # Print new column type
            print("dtype after:", props[col].dtype)
            print()

    # Print final result
    print('-' * 50)
    mem_usg = props.memory_usage().sum() / 1024**2
    print("Memory usage: {:.1f} Mb".format(mem_usg))
    print("This is {:.1f} % of the initial size".format(100 * mem_usg / start_mem_usg))
    return props, NAlist


def add_noise(series, noise_level):
    return series * (1 + noise_level * np.random.randn(len(series)))

def target_encode(trn_series=None,
                  tst_series=None,
                  target=None,
                  min_samples_leaf=1,
                  smoothing=1,
                  noise_level=0):
    """
    Smoothing is computed like in the following paper by Daniele Micci-Barreca
    https://kaggle2.blob.core.windows.net/forum-message-attachments/225952/7441/high%20cardinality%20categoricals.pdf
    trn_series : training categorical feature as a pd.Series
    tst_series : test categorical feature as a pd.Series
    target : target data as a pd.Series
    min_samples_leaf (int) : minimum samples to take category average into account
    smoothing (int) : smoothing effect to balance categorical average vs prior
    """
    assert len(trn_series) == len(target)
    assert trn_series.name == tst_series.name
    temp = pd.concat([trn_series, target], axis=1)
    # Compute target mean
    averages = temp.groupby(by=trn_series.name)[target.name].agg(["mean", "count"])
    # Compute smoothing
    smoothing = 1 / (1 + np.exp(-(averages["count"] - min_samples_leaf) / smoothing))
    # Apply average function to all target data
    prior = target.mean()
    # The bigger the count the less full_avg is taken into account
    averages[target.name] = prior * (1 - smoothing) + averages["mean"] * smoothing
    averages.drop(["mean", "count"], axis=1, inplace=True)
    # Apply averages to trn and tst series
    ft_trn_series = pd.merge(
        trn_series.to_frame(trn_series.name),
        averages.reset_index().rename(columns={'index': target.name, target.name: 'average'}),
        on=trn_series.name,
        how='left')['average'].rename(trn_series.name + '_mean').fillna(prior)
    # pd.merge does not keep the index so restore it
    ft_trn_series.index = trn_series.index
    ft_tst_series = pd.merge(
        tst_series.to_frame(tst_series.name),
        averages.reset_index().rename(columns={'index': target.name, target.name: 'average'}),
        on=tst_series.name,
        how='left')['average'].rename(trn_series.name + '_mean').fillna(prior)
    # pd.merge does not keep the index so restore it
    ft_tst_series.index = tst_series.index
    return add_noise(ft_trn_series, noise_level), add_noise(ft_tst_series, noise_level)

def check_source(sqlContext, table):

    # np = f.udf(lambda x, y: round((x / (y + 1e-10)) * 100, 1))
    table = sqlContext.table(table)
    columns = table.columns
    null_dict = dict()
    null_dict['columns'] = columns
    stage = int(0.1 * len(columns))
    null_values = []
    n = 0

    print(f"total cols: {len(columns)}")

    for i, col in enumerate(columns):
        base = table.select(col)
        n1 = base.select(col).dropna().count()
        n2 = base.select(col).count()
        pr_n = round((n2 - n1) * 100 / (n2 + 1e-10), 7)
        # if n2 - n1 > 0:
        null_values.append(pr_n)
        n += 1
        if n % stage == 0: print(f"check cols: {n}/{len(columns)}")

    null_dict['% null'] = null_values
    df = pd.DataFrame(null_dict)
    df.to_csv('chunk.csv')

'''
# Usage
    for col in cat_hight_cadrinality:
        train[col], score[col] = target_encode(train[col],
                                               score[col],
                                               train['target'],
                                               min_samples_leaf=100,
                                               smoothing=20,
                                               noise_level=0.01)
        train[col] = train[col].astype('float32')
        score[col] = score[col].astype('float32')
        print(col)
'''
