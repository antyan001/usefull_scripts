#!/home/tumanov1-ga_ca-sbrf-ru/bin/python35
import os, sys, re, time
import getpass
import teradatasql as td

USER = 'tumanov-ga'
HOST = 'tdsb15.cgs.sbrf.ru'
DB = 'PRD_DB_CLIENT4D_DEV2'
TABLE = 'nonclient_scores_2021_02_28'

sys.path.insert(0, '../')
from utils import get_pass
td_password = get_pass(True)


with td.connect(host=HOST, user=USER, password=td_password, database=DB) as db:
    cur = db.cursor()
    try:
        cur.execute("DROP TABLE {}".format(TABLE))
        db.commit()
        print('Success drop table {}'.format(TABLE))
    except:
        print("Table {} not exists".format(TABLE))


