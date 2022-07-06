import pandas as pd
import numpy as np
from IPython.display import display, HTML
from sklearn import tree
from os import system
# 사이킷런의 의사결정나무 모델을 가져옵니다.
from sklearn.tree import DecisionTreeClassifier
# 데이터를 1) 훈련, 2) 검증 데이터셋으로 분리하기 위해 사용합니다.
from sklearn.model_selection import train_test_split
# 모델의 성능을 평가하기 위해 사용합니다.
from sklearn.metrics import accuracy_score
from sklearn.svm import SVC
# import graphviz
from IPython.display import Image
# import pydotplus
import os
from sklearn import svm
# import mglearn
import pymysql
from sqlalchemy import create_engine, engine
from sqlalchemy import create_engine, engine
from information import candidate_index


# ############### 데이터 불러오기 ###############
# section_clustering_df = pd.read_csv('section_clustering_df_0701_0726.csv', index_col=0)
# df = section_clustering_df.reset_index(drop=True)
# df.drop(['Unnamed: 1'], axis=1, inplace = True)

# display(section_clustering_df)
# display(df)

# ############### 예제파일 만들기 ###############

# new_data = section_clustering_df.iloc[500]
# new_data.drop(['Unnamed: 1'], inplace = True)

def classification_svm(df, new_data):

    indexset = new_data['indexset']

    indexset_df = df[(df['indexset'] == indexset)]

    ############### SVM ###############
    x = indexset_df.iloc[:, range(1440)]
    y = indexset_df['class']

    new_ts = new_data.iloc[range(1440)]

    display(x)
    display(y)

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0)

    svm_model = SVC(kernel='linear', C=100)
    #svm_model = SVC(kernel='linear')

    # SVM 분류 모델 훈련
    svm_model.fit(x_train, y_train)

    # new data 결과 확인
    result = svm_model.predict([new_ts])

    print("예측된 라벨:", result)

    # 결과 출력
    return(new_data['indexset'], result)

# results = (classification_svm(df, new_data))
# print(results)
