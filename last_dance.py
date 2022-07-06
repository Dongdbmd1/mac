import pickle
from decimal import ROUND_FLOOR
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
from information import industry_info, candidate_index, fdrproject_cbl_db_0, fdrproject_cbl_db_1, fdrproject_cbl_db_2, fdrproject_cbl_db_3, fdrproject_cbl_db_4, fdrproject_ru_db, fdrproject_rd_db, fdrproject_rup_db, fdrproject_rdp_db
from functools import reduce
import pymysql
from sqlalchemy import create_engine, engine
import matplotlib.pyplot as plt
from sklearn.svm import SVC
# import graphviz
from IPython.display import Image
# import pydotplus
import os
from sklearn import svm


# #---------------- 데이터베이스에서 불러오기 ----------------#
# engine = create_engine(
#     'mysql+pymysql://root:0901@127.0.0.1/fdrproject', convert_unicode=True)
# conn = engine.connect()

# # 잠재량 라이브러리 불러오기

# db_data_lst = []

# for i in fdrproject_ru_db:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# ru_df = reduce(lambda left, right: pd.merge(left, right, on='PK'), db_data_lst)


# db_data_lst = []
# for i in fdrproject_rd_db:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# rd_df = reduce(lambda left, right: pd.merge(left, right, on='PK'), db_data_lst)

# db_data_lst = []
# for i in fdrproject_rup_db:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# rup_df = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)

# db_data_lst = []
# for i in fdrproject_rdp_db:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# rdp_df = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)


# # print(ru_df)
# # print(rd_df)
# # print(rup_df)
# # print(rdp_df)


# ru_df.drop(['PK'], axis=1, inplace=True)

# rd_df.drop(['PK'], axis=1, inplace=True)

# rup_df.drop(['PK'], axis=1, inplace=True)

# rdp_df.drop(['PK'], axis=1, inplace=True)

# # 기준부하 라이브러리 불러오기
# db_data_lst = []
# for i in fdrproject_cbl_db_0:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# cbl_df_0 = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)


# db_data_lst = []
# for i in fdrproject_cbl_db_1:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# cbl_df_1 = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)

# db_data_lst = []

# for i in fdrproject_cbl_db_2:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# cbl_df_2 = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)

# conn.close()

# for i in fdrproject_cbl_db_3:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# cbl_df_3 = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)

# conn.close()


# for i in fdrproject_cbl_db_4:

#     globals()['{}'.format(i)] = pd.read_sql_table('{}'.format(i), conn)
#     db_data_lst.append(globals()['{}'.format(i)])

# cbl_df_4 = reduce(lambda left, right: pd.merge(
#     left, right, on='PK'), db_data_lst)

# conn.close()


# print(cbl_df_0)
# print(cbl_df_1)
# print(cbl_df_2)

# cbl_df = pd.concat([cbl_df_0, cbl_df_1], axis=0)
# cbl_df = pd.concat([cbl_df, cbl_df_2], axis=0)
# cbl_df = pd.concat([cbl_df, cbl_df_3], axis=0)
# cbl_df = pd.concat([cbl_df, cbl_df_4], axis=0)

# # print(cbl_df)

# cbl_df.drop(['PK'], axis=1, inplace=True)
# # df = reduce(lambda left, right: pd.merge(left, right, on='PK'), db_data_lst)

# # print(df)


# # ############### 예제파일 만들기 ###############


# test_data_df = pd.read_csv('merged_preprocessed_df.csv', index_col=0)

# test_data = test_data_df.sample(n=100)


# test_data.to_csv('sample_data_n10.csv')


# # test_data = pd.read_csv('sample_data_n10.csv')
# # test_data = test_data.reset_index(drop=True)

# # copy_test_data = test_data
# print(test_data)

################

# 데이터의 max값이 달려야 하는데..........

#max_value_df = pd.read_csv('max_value.csv')


# # 관련있는 것들 묶기
# association_result_lst = [['short', 'molding_machine', 'electric_furnace', 'cast'], [
#     'mixer', 'melting_dust_collection']]

# replace_word_lst = []

# rev_candidate_index = candidate_index

# if len(association_result_lst) != 0:
#     for associ in association_result_lst:

#         associ_word = ''
#         for word in associ:
#             associ_word += word
#         replace_word_lst.append(associ_word)

#     # print(replace_word_lst)

#     copy_candidate_index = candidate_index

#     replace_keys = []
#     replace_values = []
#     for i in range(len(association_result_lst)):

#         for word in association_result_lst[i]:
#             # 데이터프레임 문자열 바꾸기
#             copy_test_data = copy_test_data.replace(word, replace_word_lst[i])

#     rev_candidate_index = copy_candidate_index

# else:
#     pass


#test_data.rename(columns = {'process': 'indexset'}, inplace = True)

# test_data['indexset'] = copy_test_data['Isholiday'] + \
#     '_' + copy_test_data['process']

# print(test_data)


#test_data.drop(['Unnamed: 0'], axis=1, inplace = True)

# print(test_data)

# 예제파일 (test_df, test_tag_df)
test_df = pd.read_csv('merged_preprocessed_df.csv', index_col=0)


merging_indus_name = '|'.join(industry_info)
test_df = test_df[test_df['industry'].str.contains(
    merging_indus_name)]
test_df = test_df.sample(n=20)
test_df.to_csv('sample_data_n20.csv')
test_df = test_df.astype({'Isholiday': 'str'})
test_df = test_df.astype({'process': 'str'})


test_tag_df = test_df[['id', 'industry', 'ymd',
                       'season', 'Isholiday', 'process', 'max']]

# 기준부하 (cbl_df, cbl_tag_df, cbl_median_df)

cbl_df = pd.read_csv('clusterized_df.csv', encoding='CP949')

# cbl_df = cbl_df.replace('h_탈지/고주파', 'h_탈지_고주파')
# cbl_df = cbl_df.replace('nh_탈지/고주파', 'h_탈지_고주파')

cbl_tag_df = cbl_df[['indexset', 'class']]
cbl_tag_df['class'] = pd.to_numeric(cbl_df['class'])
cbl_tag_df['Isholiday'] = pd.to_numeric(cbl_df['class'])


cbl_median_df = cbl_df.groupby(['indexset', 'class']).median()
test_median_df = cbl_df.groupby(['indexset', 'class']).median()
# print(cbl_median_df)

# 잠재량
ru_df = pd.read_csv('RU.csv')
rul_df = pd.read_csv('RUL.csv')
rd_df = pd.read_csv('RD.csv')
rdl_df = pd.read_csv('RDL.csv')

for k in ['ru_df', 'rul_df', 'rd_df', 'rdl_df']:

    locals()['{}'.format(k)] = locals()['{}'.format(k)].replace(
        'h_A', 'h_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기')
    locals()['{}'.format(k)] = locals()['{}'.format(k)].replace(
        'nh_A', 'nh_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기')
    locals()['{}'.format(k)] = locals()['{}'.format(
        k)].replace('h_B', 'h_믹서^용해집진#2^TR#5 HV5-M')
    locals()['{}'.format(k)] = locals()['{}'.format(k)
                                        ].replace('nh_B', 'nh_믹서^용해집진#2^TR#5 HV5-M')
    locals()['{}'.format(k)] = locals()['{}'.format(k)].replace(
        'h_C', 'h_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M')
    locals()['{}'.format(k)] = locals()['{}'.format(k)].replace(
        'nh_C', 'nh_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M')
    pass


test_df_lst = []
test_data_class_lst = []
cbl_lst = []
ramping_up_lst = []
ramping_down_lst = []
tag_lst = []


indexset_info = cbl_df.indexset.unique()

key_lst = []
value_lst = []
for i in indexset_info:
    split_lst = i.split("_")
    h_info = split_lst[0]
    p_info = split_lst[1].split("^")
    # print(p_info)

    test_divide_df = test_df.loc[test_df['Isholiday'] == h_info]

    # print(test_divide_df)
    sub_value_lst = []
    for k in p_info:
        t_df = test_divide_df.loc[test_divide_df['process'] == k]
        if len(t_df) != 0:
            sub_value_lst.append(t_df)
        else:
            pass
        pass
    # print(sub_value_lst)
    value_df = pd.DataFrame(columns=test_divide_df.columns)
    for n in sub_value_lst:
        print(n)
        value_df = pd.concat([value_df, n], axis=0)
    # print(i)
    # print(value_df)

    key_lst.append(i)
    value_lst.append(value_df)
    pass

# print(key_lst)
# print(value_lst)

a_cbl_lst = []
a_test_median_lst = []
a_indexset_lst = []
a_test_class_lst = []
a_ramping_up_lst = []
a_ramping_down_lst = []
a_id_lst = []
a_max_lst = []

for indexset_df in value_lst:
    for key in key_lst:

        indexset_df['indexset'] = key
        temp_test_df = indexset_df.reset_index(drop=True)
        print(test_df)

        temp_cbl_df = cbl_df.loc[cbl_df['indexset'] == key]
        #indexset_df.drop(['Unnamed: 0'], axis=1, inplace = True)

        # indexset_df.to_csv('aaaaaaaindexsetdf.csv')

        ############### SVM ###############
        x = temp_cbl_df.iloc[:, range(1440)]
        y = temp_cbl_df['class']
        # SVM 분류 모델 훈련
        svm_model = SVC(kernel='linear', C=100)
        svm_model.fit(x, y)

        # 해당 지표set 내에 있는 id 별로 잠재량 확인해야함
        id_info = temp_test_df.id.unique()

        for id in id_info:

            new_df = temp_test_df[temp_test_df['id'] == id]
            new_ts = new_df.iloc[:, range(1440)]

            new_ts_median = new_ts.median()

            #svm_model = SVC(kernel='linear')

            # new data 결과 확인
            result = svm_model.predict(new_ts)

            # 기준부하 구하기

            for rl in result:

                cbl = (temp_cbl_df[temp_cbl_df['class'] == rl]
                       ).iloc[:, range(0, 1440)].reset_index(drop=True)
                cbl = cbl.median()
                cbl = cbl.values.tolist()
                cbl_lst.append(cbl)

                a_test_median_lst.append(new_ts_median)
                a_indexset_lst.append(key)
                a_cbl_lst.append(cbl)
                a_test_class_lst.append(rl)
                a_id_lst.append(id)
                a_max_lst.append(new_df['max'].unique())
                pass

                # 잠재량 구하기
                ru = ru_df[(ru_df['indexset'] == key) & (ru_df['class'] ==
                                                         rl)].iloc[:, range(0, 1440)].reset_index(drop=True).median()

                rd = rd_df[(rd_df['indexset'] == key) & (rd_df['class'] ==
                                                         rl)].iloc[:, range(0, 1440)].reset_index(drop=True).median()
                rul = rul_df[(rul_df['indexset'] == key) & (rul_df['class'] ==
                                                            rl)].iloc[:, range(0, 1440)].reset_index(drop=True).median()
                rdl = rdl_df[(rdl_df['indexset'] == key) & (rdl_df['class'] ==
                                                            rl)].iloc[:, range(0, 1440)].reset_index(drop=True).median()

                ramp_up_temp_lst = []
                ramp_down_temp_lst = []

                for j in range(1440):
                    if ru[j] >= rul[j]:
                        ramp_up_temp_lst.append(rul[j])
                    else:
                        ramp_up_temp_lst.append(ru[j])

                    if rd[j] <= rdl[j]:
                        ramp_down_temp_lst.append(rdl[j])
                    else:
                        ramp_down_temp_lst.append(rd[j])

                    pass

                a_ramping_up_lst.append(ramp_up_temp_lst)
                a_ramping_down_lst.append(ramp_down_temp_lst)

            # print(test_data_lst)
            # print(test_data_class_lst)
            # print(cbl_lst)
            # print(ramping_up_lst)
            # print(ramping_down_lst)

with open("a_cbl_lst.pickle", "wb") as f:
    pickle.dump(a_cbl_lst, f)
with open("a_test_median_lst.pickle", "wb") as f:
    pickle.dump(a_test_median_lst, f)
with open("a_indexset_lst.pickle", "wb") as f:
    pickle.dump(a_indexset_lst, f)
with open("a_test_class_lst.pickle", "wb") as f:
    pickle.dump(a_test_class_lst, f)
with open("a_ramping_up_lst.pickle", "wb") as f:
    pickle.dump(a_ramping_up_lst, f)
with open("a_ramping_down_lst.pickle", "wb") as f:
    pickle.dump(a_ramping_down_lst, f)
with open("a_id_lst.pickle", "wb") as f:
    pickle.dump(a_id_lst, f)
with open("a_max_lst.pickle", "wb") as f:
    pickle.dump(a_max_lst, f)


print(a_cbl_lst)
print(a_test_median_lst)
print(a_indexset_lst)
print(a_test_class_lst)
print(a_ramping_up_lst)
print(a_ramping_down_lst)
print(a_id_lst)
print(a_max_lst)

# test_cbl_df = pd.DataFrame(cbl_lst, columns=range(1440))
# test_cbl_df = pd.concat([test_cbl_df, tag_df], axis=1)

# test_ramping_up_df = pd.DataFrame(ramping_up_lst, columns=range(1440))
# test_ramping_up_df = pd.concat([test_ramping_up_df, tag_df], axis=1)

# test_ramping_down_df = pd.DataFrame(
#     ramping_down_lst, columns=range(1440))
# test_ramping_down_df = pd.concat(
#     [test_ramping_down_df, tag_df], axis=1)

# print(test_cbl_df)
# print(test_ramping_up_df)
# print(test_ramping_down_df)

# test_cbl_df.to_csv('standard_cbl.csv')
# test_ramping_up_df.to_csv('standard_ramping_up.csv')
# test_ramping_down_df.to_csv('standard_ramping_down.csv')

# row_lst = []


# for i in range(len(test_df)):

#     new_data = test_df.iloc[i]

#     test_df_lst.append(new_data)
#     # print(new_data)

#     # indexset 찾기

#     # merged_preprocessed_df = merged_preprocessed_df[merged_preprocessed_df['industry'].str.contains(
#     #     merging_indus_name)]

#     indexset = new_data['indexset']

#     indexset_df = cbl_df[(cbl_df['indexset'] == indexset)]
#     indexset_df = indexset_df.reset_index(drop=True)
#     print(indexset_df)
#     #indexset_df.drop(['Unnamed: 0'], axis=1, inplace = True)

#     # indexset_df.to_csv('aaaaaaaindexsetdf.csv')

#     ############### SVM ###############
#     x = indexset_df.iloc[:, range(1440)]
#     y = indexset_df['class']

#     new_ts = new_data.drop(['id', 'process', 'ymd', 'industry',
#                            'week', 'max', 'Isholiday', 'season', 'indexset'])

#     display(x)
#     display(y)

#     svm_model = SVC(kernel='linear', C=100)
#     #svm_model = SVC(kernel='linear')

#     # SVM 분류 모델 훈련
#     svm_model.fit(x, y)

#     # new data 결과 확인
#     result = svm_model.predict([new_ts])

#     # print("예측된 라벨:", result)

#     test_data_class_lst.append(result)
#     pass

#     # 기준부하 구하기

#     cbl = (indexset_df[indexset_df['class'] == result[0]]
#            ).iloc[:, range(0, 1440)].reset_index(drop=True)
#     cbl = cbl.mean()
#     cbl = cbl.values.tolist()
#     # print(cbl)

#     cbl_lst.append(cbl)

#     # 잠재량 구하기
#     ru = ru_df[(ru_df['indexset'] == indexset) & (ru_df['class'] ==
#                                                   result[0])].iloc[:, range(0, 1440)].reset_index(drop=True).mean()

#     print(ru)

#     rd = rd_df[(rd_df['indexset'] == indexset) & (rd_df['class'] ==
#                                                   result[0])].iloc[:, range(0, 1440)].reset_index(drop=True).mean()
#     rup = rup_df[(rup_df['indexset'] == indexset) & (rup_df['class'] ==
#                                                      result[0])].iloc[:, range(0, 1440)].reset_index(drop=True).mean()
#     rdp = rdp_df[(rdp_df['indexset'] == indexset) & (rdp_df['class'] ==
#                                                      result[0])].iloc[:, range(0, 1440)].reset_index(drop=True).mean()

#     ramp_up_temp_lst = []
#     ramp_down_temp_lst = []

#     for j in range(1440):
#         if ru[j] > rup[j]:
#             ramp_up_temp_lst.append(rup[j])
#         else:
#             ramp_up_temp_lst.append(ru[j])

#         if rd[j] < rdp[j]:
#             ramp_down_temp_lst.append(rdp[j])
#         else:
#             ramp_down_temp_lst.append(rd[j])

#         pass

#     ramping_up_lst.append(ramp_up_temp_lst)
#     ramping_down_lst.append(ramp_down_temp_lst)


# # print(test_data_lst)
# print(test_data_class_lst)
# print(cbl_lst)
# print(ramping_up_lst)
# print(ramping_down_lst)


# test_cbl_df = pd.DataFrame(cbl_lst, columns=range(1440))
# test_cbl_df = pd.concat([test_cbl_df, tag_df], axis=1)


# test_ramping_up_df = pd.DataFrame(ramping_up_lst, columns=range(1440))
# test_ramping_up_df = pd.concat([test_ramping_up_df, tag_df], axis=1)

# test_ramping_down_df = pd.DataFrame(ramping_down_lst, columns=range(1440))
# test_ramping_down_df = pd.concat([test_ramping_down_df, tag_df], axis=1)

# print(test_cbl_df)
# print(test_ramping_up_df)
# print(test_ramping_down_df)

# test_cbl_df.to_csv('standard_cbl.csv')
# test_ramping_up_df.to_csv('standard_ramping_up.csv')
# test_ramping_down_df.to_csv('standard_ramping_down.csv')


# # max 곱하기
# normal_df_lst = []

# for i, i_df in test_cbl_df.iterrows():
#     i_df_copy = i_df
#     for j in range(144):
#         if i_df['max'] != 0:
#             i_df_copy[int('{}'.format(j))] = i_df_copy[int(
#                 '{}'.format(j))]*i_df['max']
#         else:
#             pass
#         pass
#     normal_df_lst.append(i_df_copy)
#     pass

# print(normal_df_lst[0])

# normalized_df = pd.DataFrame()

# for j in normal_df_lst:
#     j = j
#     normalized_df = pd.concat([normalized_df, j], axis=1)

#     pass
# normalized_df = normalized_df.T
# test_cbl_df = normalized_df.reset_index(drop=True)


# normal_df_lst = []

# for i, i_df in test_ramping_up_df.iterrows():
#     i_df_copy = i_df
#     for j in range(1440):
#         if i_df['max'] != 0:
#             i_df_copy[int('{}'.format(j))] = i_df_copy[int(
#                 '{}'.format(j))]*i_df['max']
#         else:
#             pass
#         pass
#     normal_df_lst.append(i_df_copy)
#     pass

# print(normal_df_lst[0])

# normalized_df = pd.DataFrame()

# for j in normal_df_lst:
#     j = j
#     normalized_df = pd.concat([normalized_df, j], axis=1)

#     pass
# normalized_df = normalized_df.T
# test_ramping_up_df = normalized_df.reset_index(drop=True)


# normal_df_lst = []

# for i, i_df in test_ramping_down_df.iterrows():
#     i_df_copy = i_df
#     for j in range(1440):
#         if i_df['max'] != 0:
#             i_df_copy[int('{}'.format(j))] = i_df_copy[int(
#                 '{}'.format(j))]*i_df['max']
#         else:
#             pass
#         pass
#     normal_df_lst.append(i_df_copy)
#     pass

# print(normal_df_lst[0])

# normalized_df = pd.DataFrame()

# for j in normal_df_lst:
#     j = j
#     normalized_df = pd.concat([normalized_df, j], axis=1)

#     pass
# normalized_df = normalized_df.T
# test_ramping_down_df = normalized_df.reset_index(drop=True)

# print(test_cbl_df)
# print(test_ramping_up_df)
# print(test_ramping_down_df)

# test_cbl_df.to_csv('output_cbl.csv')
# test_ramping_up_df.to_csv('output_ramping_up.csv')
# test_ramping_down_df.to_csv('output_ramping_down.csv')
