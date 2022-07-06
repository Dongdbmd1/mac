import pandas as pd
import numpy as np
from sklearn import datasets
from sklearn.model_selection import train_test_split
from IPython.display import display, HTML
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.cluster import KMeans
from sklearn.metrics import r2_score
#from _information import candidate_index, fdrproject_db
from functools import reduce
import pymysql
from sqlalchemy import create_engine, engine
import matplotlib.pyplot as plt

df = pd.read_csv('clusterized_df.csv', encoding='cp949')

try:
    df.drop(['Unnamed: 0'], axis=1, inplace=True)
except:
    pass
df = df.reset_index(drop=True)
df = df.replace('h_탈지/고주파', 'h_탈지_고주파')
df = df.replace('nh_탈지/고주파', 'h_탈지_고주파')

# df = df.replace(
#     'h_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기', 'h_A')
# df = df.replace(
#     'nh_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기', 'nh_A')
# df = df.replace('h_믹서^용해집진#2^TR#5 HV5-M', 'h_B')
# df = df.replace('nh_믹서^용해집진#2^TR#5 HV5-M', 'nh_B')
# df = df.replace(
#     'h_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M', 'h_C')
# df = df.replace(
#     'nh_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M', 'nh_C')


################10분 단위로 바꿔볼까 ######################
minute = 0
tag_df = df[['indexset', 'class']]
#ts_lst = []

new_df = pd.DataFrame()

while minute != 1440:

    for i in range(144):
        j = i+1
        minute += 10
        globals()['df_{}'.format(j)] = df.iloc[:, range(minute-10, minute)]
        globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
        # print(globals()['df_{}'.format(j)])
        globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].T
        globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].mean()
        globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
        # print(globals()['df_{}'.format(j)])

        new_df['{}'.format(j)] = globals()['df_{}'.format(j)]
        # print(new_df)
#       ts_lst.append(globals()['df_{}'.format(j)])
        pass
    pass

# minute_df = pd.DataFrame(ts_lst, columns = range(1,25))
# print(minute_df)

df = pd.concat([new_df, tag_df], axis=1)
# print(df)


indexset_info = df.indexset.unique()

test_df = pd.DataFrame()
train_df = pd.DataFrame()

for index_info in indexset_info:
    globals()['{}_test_df'.format(index_info)] = pd.read_csv(
        '.\\test_df\\{}_x_y_test_df.csv'.format(index_info))
    globals()['{}_test_df'.format(index_info)
              ]['indexset'] = '{}'.format(index_info)

    globals()['{}_train_df'.format(index_info)] = pd.read_csv(
        '.\\train_df\\{}_x_y_train_df.csv'.format(index_info))
    globals()['{}_train_df'.format(index_info)
              ]['indexset'] = '{}'.format(index_info)

    test_df = pd.concat(
        [test_df, globals()['{}_test_df'.format(index_info)]], axis=0)
    train_df = pd.concat(
        [train_df, globals()['{}_train_df'.format(index_info)]], axis=0)
    pass

test_df = test_df.reset_index(drop=True)
test_df.drop(['Unnamed: 0'], axis=1, inplace=True)
train_df = train_df.reset_index(drop=True)
train_df.drop(['Unnamed: 0'], axis=1, inplace=True)


test_df.to_csv('test_df.csv', index=False, encoding="CP949")
train_df.to_csv('train_df.csv', index=False, encoding="CP949")


print(len(test_df))
print(len(train_df))
#test_df = pd.read_csv('test_df.csv')


6
# print(train_df)

# #---------------- 데이터베이스에서 불러오기 ----------------#
# engine = create_engine('mysql+pymysql://root:0901@127.0.0.1/fdrproject', convert_unicode = True)
# conn = engine.connect()

# db_data_lst = []

# for i in fdrproject_db:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# conn.close()

# df = reduce(lambda left, right: pd.merge(left, right, on='PK'), db_data_lst)

# print(df)

df = train_df


def clustering_error(df):
    ############### 지표별 데이터프레임 분류 ###############

    names_info = df['indexset'].unique()

    # 결과파일 만들 list 생성
    mathod_lst = []
    error_lst = []
    class_lst = []
    in_class_lst = []
    index_name_lst = []

    ############## 지표 안 Clustering 평가 ##############
    for nm in names_info:

        indexset_df = df[df['indexset'] == nm]
        test_indexset_df = test_df[test_df['indexset'] == nm]

        display(indexset_df)

        indexset_df = indexset_df.replace([np.inf, -np.inf], None)
        indexset_df = indexset_df.dropna(axis=0)

        ############## feature와 target 정의하기 ##############
        columns = ['{}'.format(i) for i in range(1, 145)]

        x = indexset_df[columns]
        y = indexset_df['class']

        # feature와 target 합치기
        indexset_df = pd.concat([x, y], axis=1)

        # display(indexset_df)

        #class_mean_df = indexset_df.groupby(['class']).mean()
        # display(class_mean_df)

        # 중앙 값으로 대표 값 구하기

        class_mean_df = indexset_df.groupby(['class']).median()

        ############## class별 test data 평가 ##############
        count = 0

        for cl, row in class_mean_df.iterrows():
            count += 1
            plt.subplot(len(class_mean_df), 1, count)
            display(cl)
            y_true = test_indexset_df[test_indexset_df['class'] == cl][columns]
            y_pred = pd.DataFrame([row], columns=columns)

            y_pred_copy = pd.DataFrame([row], columns=columns)

            for i in range(len(indexset_df[indexset_df['class'] == cl].index)-1):
                y_pred = y_pred.append(y_pred_copy)
                #y_pred = y_pred.append(pd.DataFrame([row], columns = columns))
                pass

            # display(y_pred)
            y_true = y_true.reset_index(drop=True)
            y_pred = y_pred.reset_index(drop=True)

            # 그래프
            # for i, row in y_true.iterrows():
            #     plt.plot(range(1,145), row, 'b')
            #     pass

            # plt.title('{}_{}'.format(nm,cl))
            # plt.plot(range(1,145), y_pred.iloc[0], 'r' )

            # 평균 절대 백분율 오차 mean absolute percentage error ==> 하루 총 발전량으로 계산

            def MAPE(y, pred):
                # print(y)
                # print(pred)
                # print(y-pred)
                # print(np.abs(y-pred))

                # print(round(pred.sum(),4))
                # print(round((np.abs(y-pred)).sum()),4)

                #a = (np.abs(y-pred).sum())/len(pred)

                # y = y*100
                # pred = pred*100

                a = (np.abs(y-pred).sum())/pred.sum()

                print(a)

                b = []
                for i in range(144):
                    if (pred.sum())[i] == 0:
                        if (np.abs(y-pred).sum())[i] == 0:
                            b.append(0)
                        else:
                            pass
                            # a = a.replace([np.inf,-np.inf],None)
                            # a = a.dropna(axis=0)
                    else:
                        b.append(a[i])

                value = np.mean(b)*100
                return value

            mape = MAPE(y_true, y_pred)
            print(MAPE(y_true, y_pred))

            mathod_lst.append('MAPE')
            error_lst.append(mape)
            class_lst.append(cl)
            in_class_lst.append(len(y_true))
            index_name_lst.append('{}'.format(nm))

        # plt.legend()
        # plt.show()

    evaluation_df = pd.DataFrame({'index': index_name_lst, 'class': class_lst,
                                 'class_number': in_class_lst, 'mathod': mathod_lst, 'value': error_lst})
    display(evaluation_df)

    # csv파일로 저장
    evaluation_df.to_csv(
        'ten_minutes_Clustering_evaluation.csv', index=False, encoding='CP949')


clustering_error(df)
