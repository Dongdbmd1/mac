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
from information import candidate_index, choosing_index, industry_info
from sklearn.metrics import silhouette_score
import math
# #---------------- 피클 불러오기 ----------------#

# with open('relation_index_result_lst.pickle', 'rb') as f:
#     relation_index_result_lst = pickle.load(f)


# # 임시 #
# merged_preprocessed_df = pd.read_csv(
#     'report_preprocessed_df.csv', index_col=False, encoding = "CP949")
# try:
#     merged_preprocessed_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# except:
#     pass

# process_info = merged_preprocessed_df.process.unique()



# del_lst = []

# for i in process_info:
#     process_df = merged_preprocessed_df[merged_preprocessed_df['process']==i]
#     ts_process_df = process_df.iloc[:,range(1440)]
#     print(ts_process_df.sum().sum())
#     if ts_process_df.sum().sum() == 0:
#         del_lst.append(i)
#     else:
#         pass

# print(del_lst)

# merging_del_name = '|'.join(del_lst)
# merged_preprocessed_df = merged_preprocessed_df[merged_preprocessed_df['process'].str.contains(
#     merging_del_name)]

# merging_indus_name = '|'.join(industry_info)
# merged_preprocessed_df = merged_preprocessed_df[merged_preprocessed_df['industry'].str.contains(
#     merging_indus_name)]




# # 후보지표 분석 결과에 따른 세부지표 묶기
# relation_index_result_lst = [['short', 'molding_machine', 'electric_furnace', 'cast'],['mixer', 'melting_dust_collection']]


def section_clustering(merged_preprocessed_df, relation_index_result_lst, candidate_index, choosing_index):

    copy_candidate_index = candidate_index
    copy_choosing_index = choosing_index

    #---------------- 데이터프레임과 후보지표의 세부지표를 연관되어있는 것끼리 묶어서 수정하기 ----------------#

    # 후보지표 결과를 통해 알게된 관련있는 것들 묶기
    replace_word_lst = []

    # 만약 관련된 세부지표가 0이 아니라면
    if len(relation_index_result_lst) != 0:
        # 세부지표묶음 돌기
        for associ in relation_index_result_lst:
            associ_word = ''
            # 세부지표묶음 내 세부지표 단어 합치기
            for word in associ:
                if associ[-1] != word:
                    associ_word += word + '^'

                else:
                    associ_word += word
            replace_word_lst.append(associ_word)

        print(replace_word_lst)

        # 딕셔너리 수정을 위해 key값 재정의
        replace_keys = []
        # 딕셔너리 수정을 위한 values 값 재 정의
        replace_values = []

        for i in range(len(relation_index_result_lst)):

            for word in relation_index_result_lst[i]:
                # 데이터프레임 문자열 바꾸기
                merged_preprocessed_df = merged_preprocessed_df.replace(
                    word, replace_word_lst[i])

            # 딕셔너리 문자열 바꾸기
            for keys, values in copy_candidate_index.items():
                replace_keys.append(keys)
                sub_replace_values = []

                if relation_index_result_lst[i][0] in values:
                    for v in values:
                        if v in relation_index_result_lst[i]:
                            sub_replace_values.append(replace_word_lst[i])
                        else:
                            sub_replace_values.append(v)
                        pass
                else:
                    sub_replace_values = values

                replace_values.append(sub_replace_values)

        settled_index = dict(zip(replace_keys, tuple(replace_values)))

        settled_index['industry'] = set(settled_index['industry'])

    else:
        pass

    print(replace_keys)
    print(replace_values)
    print(settled_index)
    print(merged_preprocessed_df)

    # 지표 수정한 데이터프레임 csv파일로 저장하기
    revised_index_df = merged_preprocessed_df
    revised_index_df.to_csv('revised_index_df.csv',
                            encoding='CP949', index=False)

    #---------------- 지표별로 Clustering하기 위해 지표 나누기  ----------------#

    # 지표별 데이터프레임 저장
    ori_all_df = []

    # 지표별 시계열 데이터의 데이터프레임 저장
    ts_all_df = []

    # 데이터프레임 이름 저장
    names_all_df = []

    process_info = revised_index_df.process.unique()

    # print(process_info)

    # '휴일유무, 계절'으로 지표셋을 형성하여 데이터프레임을 분류할 경우
    if 'Isholiday' in copy_choosing_index and 'process' not in copy_choosing_index and 'season' in copy_choosing_index and 'industry' not in copy_choosing_index:
        for hnh in settled_index['Isholiday']:
            # 지표에 사용될 휴일 유무에 따른 데이터 프레임 분류(Isholiday)
            locals()['{}_df'.format(
                hnh)] = merged_preprocessed_df.loc[merged_preprocessed_df['Isholiday'] == '{}'.format(hnh)]
            for ss in settled_index['season']:
                # 지표 별 데이터 프레임 구분(season)
                locals()['{}_{}'.format(hnh, ss)] = locals()['{}_df'.format(
                    hnh)].loc[locals()['{}_df'.format(hnh)]['season'] == '{}'.format(ss)]

                # 지표셋 칼럼 생성
                locals()['{}_{}'.format(hnh, ss)
                         ]['indexset'] = '{}_{}'.format(hnh, ss)

                # 사용안할 칼럼 삭제
                locals()['{}_{}'.format(hnh, ss)] = locals()['{}_{}'.format(hnh, ss)].drop(
                    ['ymd', 'id', 'season', 'industry', 'week', 'max', 'process', 'id', 'ymd', 'Isholiday'], axis=1)

                # 모든 데이터프레임 all_df에 담기
                ori_all_df.append(locals()['{}_{}'.format(hnh, ss)])

                # Clustering을 위해 시계열 데이터만 담은 데이터프레임 생성
                locals()['ts_{}_{}'.format(hnh, ss)] = locals()[
                    '{}_{}'.format(hnh, ss)].drop(['indexset'], axis=1)
                ts_all_df.append(locals()['ts_{}_{}'.format(hnh, ss)])
                names_all_df.append('{}_{}'.format(hnh, ss))
            pass

    # '휴일유무, 공정'으로 지표셋을 형성하여 데이터프레임을 분류할 경우
    if 'Isholiday' in copy_choosing_index and 'process' in copy_choosing_index and 'season' not in copy_choosing_index and 'industry' not in copy_choosing_index:
        for hnh in settled_index['Isholiday']:
            # 지표에 사용될 휴일 유무에 따른 데이터 프레임 분류(Isholiday)
            locals()['{}_df'.format(
                hnh)] = merged_preprocessed_df.loc[merged_preprocessed_df['Isholiday'] == '{}'.format(hnh)]
            for pro in process_info:
                # 지표 별 데이터 프레임 구분(process)
                locals()['{}_{}'.format(hnh, pro)] = locals()['{}_df'.format(
                    hnh)].loc[locals()['{}_df'.format(hnh)]['process'] == '{}'.format(pro)]

                # 지표셋 칼럼 생성
                locals()['{}_{}'.format(hnh, pro)
                         ]['indexset'] = '{}_{}'.format(hnh, pro)

                # 사용안할 칼럼 삭제
                locals()['{}_{}'.format(hnh, pro)] = locals()['{}_{}'.format(hnh, pro)].drop(
                    ['ymd', 'id', 'season', 'industry', 'week', 'max', 'process', 'id', 'ymd', 'Isholiday'], axis=1)

                # 모든 데이터프레임 all_df에 담기
                ori_all_df.append(locals()['{}_{}'.format(hnh, pro)])

                # Clustering을 위해 시계열 데이터만 담은 데이터프레임 생성
                locals()['ts_{}_{}'.format(hnh, pro)] = locals()[
                    '{}_{}'.format(hnh, pro)].drop(['indexset'], axis=1)
                ts_all_df.append(locals()['ts_{}_{}'.format(hnh, pro)])
                names_all_df.append('{}_{}'.format(hnh, pro))
            pass

    # '계절, 공장, 휴일유무, 공정'으로 지표셋을 형성하여 데이터프레임을 분류할 경우
    elif 'Isholiday' in copy_choosing_index and 'process' in copy_choosing_index and 'season' in copy_choosing_index and 'industry' in copy_choosing_index:

        for ss in settled_index['season']:
            # 지표에 사용될 계절 별 데이터 프레임 분류(season)
            locals()['{}_df'.format(ss)
                     ] = merged_preprocessed_df.loc[merged_preprocessed_df['season'] == '{}'.format(ss)]
            for indus in settled_index['industry']:
                # 지표 별 데이터 프레임 구분(industry)
                locals()['{}_{}'.format(ss, indus)] = locals()['{}_df'.format(
                    ss)].loc[locals()['{}_df'.format(ss)]['industry'] == '{}'.format(indus)]
                for hnh in settled_index['Isholiday']:
                    locals()['{}_{}_{}'.format(ss, indus, hnh)] = locals()['{}_{}'.format(
                        ss, indus)].loc[locals()['{}_{}'.format(ss, indus)]['Isholiday'] == '{}'.format(hnh)]

                    # 지표 별 데이터 프레임 구분(process)
                    for pro in settled_index['process']:
                        locals()['{}_{}_{}_{}'.format(ss, indus, hnh, pro)] = locals()['{}_{}_{}'.format(
                            ss, indus, hnh)].loc[locals()['{}_{}_{}'.format(ss, indus, hnh)]['process'] == '{}'.format(pro)]

                        # 지표셋 칼럼 생성
                        locals()['{}_{}_{}_{}'.format(
                            ss, indus, hnh, pro)]['indexset'] = '{}_{}_{}_{}'.format(ss, indus, hnh, pro)

                        # 사용안할 칼럼 삭제
                        locals()['{}_{}_{}_{}'.format(ss, indus, hnh, pro)] = locals()['{}_{}_{}_{}'.format(ss, indus, hnh, pro)].drop(
                            ['Isholiday', 'season', 'industry', 'process', 'week', 'max', 'process', 'id', 'ymd'], axis=1)

                        # 모든 데이터프레임 all_df에 담기
                        ori_all_df.append(
                            locals()['{}_{}_{}_{}'.format(ss, indus, hnh, pro)])

                        # Clustering을 위해 시계열 데이터만 담은 데이터프레임 생성
                        locals()['ts_{}_{}_{}_{}'.format(ss, indus, hnh, pro)] = locals()[
                            '{}_{}_{}_{}'.format(ss, indus, hnh, pro)].drop(['indexset'], axis=1)
                        ts_all_df.append(
                            locals()['ts_{}_{}_{}_{}'.format(ss, indus, hnh, pro)])
                        names_all_df.append(
                            '{}_{}_{}_{}'.format(ss, indus, hnh, pro))
                        pass
                    pass
            pass
    # '계절, 공장, 휴일유무'로 지표셋을 형성하여 데이터프레임을 분류할 경우
    elif 'Isholiday' in copy_choosing_index and 'process' not in copy_choosing_index and 'season' in copy_choosing_index and 'industry' in copy_choosing_index:

        for ss in settled_index['season']:
            # 지표에 사용될 계절 별 데이터 프레임 분류(season)
            locals()['{}_df'.format(ss)
                     ] = merged_preprocessed_df.loc[merged_preprocessed_df['season'] == '{}'.format(ss)]
            for indus in settled_index['industry']:
                # 지표 별 데이터 프레임 구분(industry)
                locals()['{}_{}'.format(ss, indus)] = locals()['{}_df'.format(
                    ss)].loc[locals()['{}_df'.format(ss)]['industry'] == '{}'.format(indus)]
                for hnh in settled_index['Isholiday']:
                    locals()['{}_{}_{}'.format(ss, indus, hnh)] = locals()['{}_{}'.format(
                        ss, indus)].loc[locals()['{}_{}'.format(ss, indus)]['Isholiday'] == '{}'.format(hnh)]
                    # 지표셋 칼럼 생성
                    locals()['{}_{}_{}'.format(ss, indus, hnh)
                             ]['indexset'] = '{}_{}_{}'.format(ss, indus, hnh)

                    # 사용안할 칼럼 삭제
                    locals()['{}_{}_{}'.format(ss, indus, hnh)] = locals()['{}_{}_{}'.format(ss, indus, hnh)].drop(
                        ['Isholiday', 'season', 'industry', 'week', 'max', 'process', 'id', 'ymd'], axis=1)

                    # 모든 데이터프레임 all_df에 담기
                    ori_all_df.append(
                        locals()['{}_{}_{}'.format(ss, indus, hnh)])

                    print(locals()['{}_{}_{}'.format(ss, indus, hnh)])
                    # Clustering을 위해 시계열 데이터만 담은 데이터프레임 생성

                    locals()['ts_{}_{}_{}'.format(ss, indus, hnh)] = locals()[
                        '{}_{}_{}'.format(ss, indus, hnh)].drop(['indexset'], axis=1)
                    ts_all_df.append(
                        locals()['ts_{}_{}_{}'.format(ss, indus, hnh)])
                    names_all_df.append('{}_{}_{}'.format(ss, indus, hnh))
                    pass
                pass
            pass

    # '계절, 공정, 휴일유무'로 지표셋을 형성하여 데이터프레임을 분류할 경우
    elif 'Isholiday' in copy_choosing_index and 'process' in copy_choosing_index and 'season' in copy_choosing_index and 'industry' not in copy_choosing_index:

        for ss in settled_index['season']:
            # 지표에 사용될 계절 별 데이터 프레임 분류(season)
            locals()['{}_df'.format(ss)
                     ] = merged_preprocessed_df.loc[merged_preprocessed_df['season'] == '{}'.format(ss)]
            for pro in settled_index['process']:
                # 지표 별 데이터 프레임 구분(process)
                locals()['{}_{}'.format(ss, pro)] = locals()['{}_df'.format(
                    ss)].loc[locals()['{}_df'.format(ss)]['process'] == '{}'.format(pro)]
                for hnh in settled_index['Isholiday']:
                    locals()['{}_{}_{}'.format(ss, pro, hnh)] = locals()['{}_{}'.format(
                        ss, pro)].loc[locals()['{}_{}'.format(ss, pro)]['Isholiday'] == '{}'.format(hnh)]
                    # 지표셋 칼럼 생성
                    locals()['{}_{}_{}'.format(ss, pro, hnh)
                             ]['indexset'] = '{}_{}_{}'.format(ss, pro, hnh)

                    # 사용안할 칼럼 삭제
                    locals()['{}_{}_{}'.format(ss, pro, hnh)] = locals()['{}_{}_{}'.format(ss, pro, hnh)].drop(
                        ['Isholiday', 'season', 'industry', 'week', 'max', 'process', 'id', 'ymd'], axis=1)

                    # 모든 데이터프레임 all_df에 담기
                    ori_all_df.append(
                        locals()['{}_{}_{}'.format(ss, pro, hnh)])

                    print(locals()['{}_{}_{}'.format(ss, pro, hnh)])
                    # Clustering을 위해 시계열 데이터만 담은 데이터프레임 생성

                    locals()['ts_{}_{}_{}'.format(ss, pro, hnh)] = locals()[
                        '{}_{}_{}'.format(ss, pro, hnh)].drop(['indexset'], axis=1)
                    ts_all_df.append(
                        locals()['ts_{}_{}_{}'.format(ss, pro, hnh)])
                    names_all_df.append('{}_{}_{}'.format(ss, pro, hnh))
                    pass
                pass
            pass

    print(names_all_df)
    print(len(names_all_df))

    ###################### 딕셔너리 만들기  ######################
    ts_all_df_dict = dict.fromkeys(names_all_df)
    ori_all_df_dict = dict.fromkeys(names_all_df)

    j = 0
    for i in ts_all_df_dict.keys():
        ts_all_df_dict[i] = ts_all_df[j]
        j += 1
        pass

    j = 0
    for i in ori_all_df_dict.keys():
        ori_all_df_dict[i] = ori_all_df[j]
        j += 1
        pass

    display(ori_all_df_dict)
    display(ts_all_df_dict)

    #---------------- 지표셋별 Clustering 하기 ----------------#

    ###################### silhouette 기법으로 알맞은 centroid 구하기(수치) ######################

    indexset_name_lst = []
    k_number_lst = []
    k_number_score_lst = []
    ts_df_lst = []

    for key, ts_df in ts_all_df_dict.items():
        score_lst = []
        # 만일 ts_df가 10개 이상일 때를 작성해야함
        if len(ts_df) >= 10:
            for i in range(2, 6):
                km = KMeans(n_clusters=i, init='k-means++', random_state=0)
                km.fit(ts_df)
                cluster = km.predict(ts_df)
                score = silhouette_score(ts_df, cluster)
                score_lst.append(score)
                pass

            k_number = 0
            k_number_score = 0
            for i in range(len(score_lst)):
                if i > 1:

                    if score_lst[i] >= k_number_score:
                        k_number = i+1
                        k_number_score = score_lst[i]
                    else:
                        pass
                else:
                    pass
                pass

            indexset_name_lst.append(key)
            k_number_lst.append(k_number)
            k_number_score_lst.append(k_number_score)
            ts_df_lst.append(ts_df)

        else:
            indexset_name_lst.append(key)
            k_number_lst.append(1)
            k_number_score_lst.append("데이터10개미만")
            ts_df_lst.append(ts_df)

    # print(indexset_name_lst)
    # print(k_number_lst)
    # print(k_number_score_lst)

    ################## Kmeans 기법 적용 ###################
    class_num = k_number_lst
    class_info = []
    count = 1
    index = 0

    for key, i in ts_all_df_dict.items():
        # plt.subplot(int(round(math.sqrt(len(k_number_lst)),0)),int(round(math.sqrt(len(k_number_lst)),0))+1, count)
        if i.empty == False:
            model = KMeans(
                n_clusters=class_num[index], init='k-means++', random_state=0)

            i = i.replace([np.inf, -np.inf], 0)

            model.fit(i)

            Results = model.fit_predict(i)
            i['class'] = Results

            ori_all_df_dict['{}'.format(key)]['class'] = Results
            class_mean = i.groupby(['class']).mean()
            class_mean_T = class_mean.T
            # plt.title('{}'.format(key))
            # plt.xlabel('hour')
            # plt.ylabel('SGD')
            # for k in range(class_num[index]):
            #     plt.plot(class_mean_T[k], label='group {}'.format(k))
            # pass

            count += 1
            index += 1
        else:
            # plt.title('{}_<EMPTY>'.format(key))
            # plt.xlabel('hour')
            # plt.ylabel('SGD')
            count += 1
            index += 1
            pass

    # plt.show()

    # print(count)

    #---------------- class열이 합쳐진 지표셋별 데이터프레임들을 하나의 데이터프레임으로 합치기  ----------------#
    clusterized_df = pd.concat(ori_all_df_dict, axis=0)

    # display(clusterized_df)
    clusterized_df = clusterized_df.reset_index(drop=True)
    # csv파일로 저장
    clusterized_df.to_csv('clusterized_df.csv', index=False, encoding='cp949')

    return clusterized_df


# section_clustering(merged_preprocessed_df,
#                    relation_index_result_lst, candidate_index, choosing_index)
 