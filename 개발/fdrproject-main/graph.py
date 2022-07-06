import pandas as pd
import matplotlib.pyplot as plt
#import seaborn as sns
import numpy as np
#import random as rd
from IPython.display import display, HTML
from sklearn.cluster import KMeans
#from sklearn.metrics import davies_bouldin_score
#import pymysql
from sqlalchemy import create_engine, engine
import pickle
from information import candidate_index, candidate_index_checking, industry_info
from sklearn.metrics import silhouette_score

# df = open("merged_preprocessed_df.pickle", "rb")
# merged_preprocessed_df = pickle.load(df)
# df.close()


# merged_preprocessed_df = pd.read_csv(
#     "merged_preprocessed_df.csv", index_col=False)
# plt.rc('font', family='Malgun Gothic')


# merging_indus_name = '|'.join(industry_info)
# merged_preprocessed_df = merged_preprocessed_df[merged_preprocessed_df['industry'].str.contains(
#     merging_indus_name)]


# process_info = merged_preprocessed_df.process.unique()


# del_lst = []

# for i in process_info:
#     process_df = merged_preprocessed_df[merged_preprocessed_df['process'] == i]
#     ts_process_df = process_df.iloc[:, range(1440)]
#     print(ts_process_df.sum().sum())
#     if ts_process_df.sum().sum() == 0:
#         del_lst.append(i)
#     else:
#         pass

# print(del_lst)

# for dl in del_lst:
#     merged_preprocessed_df = merged_preprocessed_df[merged_preprocessed_df['process'] != dl]
#     pass


# merged_preprocessed_df[['year', 'month', 'day']] = pd.DataFrame(
#     merged_preprocessed_df.ymd.str.split('-', 2).tolist())

# for j in ['01', '02', '03', '04', '05', '06']:
#     locals()['{}_lst'.format(j)] = []
#     for i in merged_preprocessed_df.ymd.unique():

#         if i[5:7] == j:
#             locals()['{}_lst'.format(j)].append(i)
#         else:
#             pass
#     print(j, len(locals()['{}_lst'.format(j)]))

#     pass


# print('총 날짜 수', len(merged_preprocessed_df.ymd.unique()))
# print('총 부하 수 ', len(merged_preprocessed_df.id.unique()))

# #---------------- 분단위 데이터 한시간 단위로 바꾸기 ----------------#
# minute = 0
# tag_df = merged_preprocessed_df[[
#     'id', 'process', 'ymd', 'industry', 'week', 'Isholiday', 'season']]

# hour_df = pd.DataFrame()

# while minute != 1440:

#     for i in range(24):
#         # 시간 1부터 세기 위해서
#         j = i+1
#         minute += 60
#         globals()['df_{}'.format(
#             j)] = merged_preprocessed_df.iloc[:, range(minute-60, minute)]
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].T
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].mean()
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         hour_df['{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(hour_df)
#         pass
#     pass

# hour_df = pd.concat([hour_df, tag_df], axis=1)
# # print(hour_df)

# ####### industry 별 그래프 그려보기 ###########

# industry_info = hour_df['industry'].unique()
# name = []
# value = []
# count = 1


# for indus in industry_info:
#     plt.subplot(7, 3, count)
#     draw_dt = hour_df[hour_df['industry'] == indus]

#     # 시간데이터만 추출
#     draw_dt_ts = draw_dt.iloc[:, range(24)]
#     # 스케일업
#     draw_dt_ts = draw_dt_ts*100
#     # Nan 값은 0으로 대체
#     draw_dt_ts = draw_dt_ts.replace([np.inf, -np.inf], 0)
#     # 평균값 구하기
#     draw_dt_ts_mean = draw_dt_ts.mean()

#     # print(draw_dt_ts)

#     name.append(indus)
#     value.append(draw_dt_ts_mean)

#     # print(draw_dt_ts_mean)

#     for i in range(len(draw_dt_ts)):

#         dt = draw_dt_ts.iloc[i]
#         plt.plot(dt)

#     pass

#     plt.title('{}_{}개'.format(indus, len(draw_dt_ts)))
#     plt.xticks(range(24))
#     plt.ylabel('use')
#     count += 1

# plt.show()
# print(draw_dt_ts_mean)

# # ---------------- 한시간 단위로 되있는 데이터 후보지표별로 그래프 작성하여 확인 - ---------------

# df = hour_df
# count = 1

# # 후보지표별로 데이터 추출해서 후보지표 내 요소들의 평균값 구하기

# for i in candidate_index.keys():
#     plt.subplot(4, 1, count)
#     name = []
#     value = []

#     # 해당 후보지표를 보유한 데이터 추출
#     for j in candidate_index[i]:
#         draw_dt = df[df[i] == j]

#         # 시간데이터만 추출
#         draw_dt_ts = draw_dt.iloc[:, range(24)]
#         # 스케일업
#         draw_dt_ts = draw_dt_ts*100
#         # Nan 값은 0으로 대체
#         draw_dt_ts = draw_dt_ts.replace([np.inf, -np.inf], 0)
#         # 평균값 구하기
#         draw_dt_ts_mean = draw_dt_ts.mean()

#         # print(draw_dt_ts)

#         name.append(j)
#         value.append(draw_dt_ts_mean)

#         # print(draw_dt_ts_mean)

#         plt.title('{}'.format(i))
#         plt.xticks(range(24))
#         plt.ylabel('use')
#         plt.plot(draw_dt_ts_mean, label='{}'.format(j))
#         plt.legend()
#         pass
#     count += 1

# plt.show()

# print(hour_df_ts)


################################# section cluster ###############################

# clusterized_df = pd.read_csv(
#     'clusterized_df.csv', index_col=False, encoding='cp949')

# plt.rc('font', family='Malgun Gothic')


# try:
#     clusterized_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# except:
#     pass


# clusterized_df = clusterized_df.replace(
#     'h_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기', 'h_A')
# clusterized_df = clusterized_df.replace(
#     'nh_콤프레셔 #1^사처리^쇼트기^냉각수^현장집진^350HP 컴프레셔^냉동기 1000RT 3호^컴프레셔 350HP^터보냉동기^RX발생기', 'nh_A')
# clusterized_df = clusterized_df.replace('h_믹서^용해집진#2^TR#5 HV5-M', 'h_B')
# clusterized_df = clusterized_df.replace('nh_믹서^용해집진#2^TR#5 HV5-M', 'nh_B')
# clusterized_df = clusterized_df.replace(
#     'h_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M', 'h_C')
# clusterized_df = clusterized_df.replace(
#     'nh_컴프레셔 100HP^냉각수 펌프시설^TR#1 LV1-M', 'nh_C')


# #---------------- 분단위 데이터 한시간 단위로 바꾸기 ----------------#
# minute = 0
# tag_df = clusterized_df[[
#     'indexset', 'class']]

# hour_df = pd.DataFrame()

# while minute != 1440:

#     for i in range(24):
#         # 시간 1부터 세기 위해서
#         j = i+1
#         minute += 60
#         globals()['df_{}'.format(
#             j)] = clusterized_df.iloc[:, range(minute-60, minute)]
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].T
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].mean()
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         hour_df['{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(hour_df)
#         pass
#     pass

# hour_df = pd.concat([hour_df, tag_df], axis=1)
# # print(hour_df)

# print(len(clusterized_df['indexset'].unique()))

# ####### indexset 별 그래프 그려보기 ###########

# induexset_info = hour_df['indexset'].unique()
# name = []
# value = []
# count = 1


# # indexset별 군집별 평균

# for i in induexset_info:
#     plt.subplot(6, 7, count)
#     draw_dt = hour_df[hour_df['indexset'] == i]

#     class_info = hour_df['class'].unique()
#     # 군집별

#     class_number = 0
#     for cl in class_info:
#         draw_dt_cl = draw_dt[draw_dt['class'] == cl]

#         # 시간데이터만 추출
#         draw_dt_ts = draw_dt_cl.iloc[:, range(24)]
#         # 스케일업
#         draw_dt_ts = draw_dt_ts*100
#         # Nan 값은 0으로 대체
#         draw_dt_ts = draw_dt_ts.replace([np.inf, -np.inf], 0)

#         # 평균값 구하기
#         draw_dt_ts_mean = draw_dt_ts.mean()

#         plt.plot(draw_dt_ts_mean)
#         plt.legend()

#         class_number += len(draw_dt_ts)
#         pass
#     ax = plt.gca()
#     ax.axes.xaxis.set_visible(False)
#     plt.title('{}_{}개'.format(i, class_number))
#     plt.xticks(range(24))
#     plt.ylabel('use')
#     count += 1

# plt.show()
# print(draw_dt_ts_mean)


################################# 지표별  ###############################

# df = pd.read_csv(
#     'merged_preprocessed_df.csv', index_col=False)

# plt.rc('font', family='Malgun Gothic')


# try:
#     df.drop(['Unnamed: 0'], axis=1, inplace=True)
# except:
#     pass

# merging_indus_name = '|'.join(industry_info)
# df = df[df['industry'].str.contains(
#     merging_indus_name)]


# process_info = df.process.unique()


# del_lst = []

# for i in process_info:
#     process_df = df[df['process'] == i]
#     ts_process_df = process_df.iloc[:, range(1440)]
#     print(ts_process_df.sum().sum())
#     if ts_process_df.sum().sum() == 0:
#         del_lst.append(i)
#     else:
#         pass

# print(del_lst)

# for dl in del_lst:
#     df = df[df['process'] != dl]
#     pass
# #---------------- 분단위 데이터 한시간 단위로 바꾸기 ----------------#
# minute = 0
# tag_df = df[[
#     'industry', 'Isholiday', 'season', 'process']]

# hour_df = pd.DataFrame()

# while minute != 1440:

#     for i in range(24):
#         # 시간 1부터 세기 위해서
#         j = i+1
#         minute += 60
#         globals()['df_{}'.format(
#             j)] = df.iloc[:, range(minute-60, minute)]
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].T
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)].mean()
#         globals()['df_{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(globals()['df_{}'.format(j)])
#         hour_df['{}'.format(j)] = globals()['df_{}'.format(j)]
#         # print(hour_df)
#         pass
#     pass

# hour_df = pd.concat([hour_df, tag_df], axis=1)
# print(hour_df)


# ####### tag 별 그래프 그려보기 ###########

# #tag_info = ['Isholiday', 'season', 'process', 'industry']

# tag_info = ['industry']
# for t in tag_info:

#     t_info = hour_df['{}'.format(t)].unique()
#     name = []
#     value = []
#     count = 1

#     # indexset별 군집별 평균

#     for i in t_info:
#         # plt.subplot(round(len(t_info)**(1/2)),
#         #             round(len(t_info)**(1/2))+1, count)
#         plt.subplot(2, 2, count)
#         draw_dt = hour_df[hour_df['{}'.format(t)] == i]

#         # 시간데이터만 추출
#         draw_dt_ts = draw_dt.iloc[:, range(24)]
#         # 스케일업
#         draw_dt_ts = draw_dt_ts*100
#         # Nan 값은 0으로 대체
#         draw_dt_ts = draw_dt_ts.replace([np.inf, -np.inf], 0)

#         # 평균값 구하기
#         draw_dt_ts_mean = draw_dt_ts.mean()

#         plt.plot(draw_dt_ts_mean)
#         # plt.legend()
#         # ax = plt.gca()
#         # ax.axes.xaxis.set_visible(False)
#         plt.title('{}'.format(i))
#         # plt.xticks(range(24))
#         # plt.ylabel('use')
#         count += 1

#     plt.show()
#     print(draw_dt_ts_mean)

with open("a_cbl_lst.pickle", "rb") as fr:
    cbl_lst = pickle.load(fr)
with open("a_test_median_lst.pickle", "rb") as fr:
    test_median_lst = pickle.load(fr)
with open("a_indexset_lst.pickle", "rb") as fr:
    indexset_lst = pickle.load(fr)
with open("a_test_class_lst.pickle", "rb") as fr:
    test_class_lst = pickle.load(fr)
with open("a_ramping_up_lst.pickle", "rb") as fr:
    ramping_up_lst = pickle.load(fr)
with open("a_ramping_down_lst.pickle", "rb") as fr:
    ramping_down_lst = pickle.load(fr)
with open("a_id_lst.pickle", "rb") as fr:
    id_lst = pickle.load(fr)
with open("a_max_lst.pickle", "rb") as fr:
    max_lst = pickle.load(fr)

print(len(test_median_lst))
for i in range(len(test_median_lst)):

    print(test_median_lst[i])
    print(test_median_lst[i]*max_lst[i][0])
    plt.title('{}'.format(id_lst[i]))
    print(indexset_lst[i])

    plt.plot(test_median_lst[i])
    plt.plot(cbl_lst[i])
    plt.plot(ramping_up_lst[i])
    plt.plot(ramping_down_lst[i])
    ax = plt.gca()
    ax.axes.xaxis.set_visible(False)
    plt.legend(('new_data', 'cbl', 'ramping_up', 'ramping_down'))
    plt.show()

    #     # 시간데이터만 추출
    #     draw_dt_ts = draw_dt.iloc[:, range(24)]
    #     # 스케일업
    #     draw_dt_ts = draw_dt_ts*100
    #     # Nan 값은 0으로 대체
    #     draw_dt_ts = draw_dt_ts.replace([np.inf, -np.inf], 0)

    #     # 평균값 구하기
    #     draw_dt_ts_mean = draw_dt_ts.mean()

    #     plt.plot(draw_dt_ts_mean)
    #     # plt.legend()
    #     # ax = plt.gca()
    #     # ax.axes.xaxis.set_visible(False)
    #     plt.title('{}'.format(i))
    #     # plt.xticks(range(24))
    #     # plt.ylabel('use')
    #     count += 1

    # plt.show()
    # print(draw_dt_ts_mean)
