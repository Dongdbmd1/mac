import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import random as rd
from IPython.display import display, HTML
from sklearn.cluster import KMeans
from sklearn.metrics import davies_bouldin_score
import re
import pymysql
from sqlalchemy import create_engine, engine
import pickle
from information import fdrproject_db
from functools import reduce

from classification_svm import classification_svm


#---------------- 데이터베이스에서 불러오기 ----------------#
engine = create_engine(
    'mysql+pymysql://root:0901@127.0.0.1/fdrproject', convert_unicode=True)
conn = engine.connect()

db_data_lst = []

for i in fdrproject_db:

    globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
    db_data_lst.append(globals()['{}'.format(i)])

conn.close()

df_merge = reduce(lambda left, right: pd.merge(
    left, right, on='PK'), db_data_lst)

print(df_merge)


# 데이터 넣기 1440분 시계열 데이터, indexset(ex. summer_buchen_h)
new_data = []

classification_svm(df_merge, new_data)
