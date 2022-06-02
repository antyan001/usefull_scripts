#!/home/$USER/bin/python36
import os, sys, re, time
import getpass
import teradatasql as td

USER = ''
HOST = 'tdsb15.cgs.sbrf.ru'
DB = 'PRD_DB_CLIENT4D_REP'
TABLE = 'stg_org_decision_maker'
TO_USER = ''

sys.path.insert(0, '../')
from utils import get_pass
td_password = get_pass(True)


with td.connect(host=HOST, user=USER, password=td_password, database=DB, logmech='LDAP') as db:
    cur = db.cursor()
    sql_str = f"GRANT SELECT ON {TABLE} TO \"{TO_USER}\""
    print(sql_str)
    cur.execute(sql_str)
    db.commit()
    print(f'Success grant table {TABLE} to {TO_USER}')

