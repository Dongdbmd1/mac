import pandas as pd
import matplotlib.pyplot as plt
#import seaborn as sns
import numpy as np
#import random as rd
from IPython.display import display, HTML
from sklearn.cluster import KMeans
#from sklearn.metrics import davies_bouldin_score
import pymysql
from sqlalchemy import create_engine, engine
import pickle
from information import candidate_index, candidate_index_checking, industry_info
from sklearn.metrics import silhouette_score

# df = open("..//merged_preprocessed_df.pickle", "rb")
# merged_preprocessed_df = pickle.load(df)
# df.close()
# plt.rc('font', family='Malgun Gothic')

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

# merged_preprocessed_df.to_csv("report_preprocessed_df.csv", encoding= "CP949", index = False)

def index_selecting(preprocessed_df, candidate_index, candidate_index_checking):
    # industry_info = ['SKC 도금공장', '케이아이씨', '라파스',
    #                  '중외제약', '부천주물', '대창 후문공장', '율촌2공장', '부성금속공업사']

    # merging_indus_name = '|'.join(industry_info)
    # preprocessed_df = preprocessed_df[preprocessed_df['industry'].str.contains(
    #     merging_indus_name)]
    # print(len(preprocessed_df))

    # report_process = preprocessed_df['process'].unique()

    # report_process_df = pd.DataFrame({'process': report_process})

    # report_process_df.to_csv(
    #     'report_process.csv', index=False, encoding="CP949")

    #---------------- 분단위 데이터 한시간 단위로 바꾸기 ----------------#
    '''
    Change time resolution from a minute to a hour
    '''

    minute = 0
    tag_df = preprocessed_df[[
        'id', 'process', 'ymd', 'industry', 'week', 'Isholiday', 'season']]

    hour_df = pd.DataFrame()

    while minute != 1440:

        for i in range(24):
            # 시간 1부터 세기 위해서
            j = i+1
            minute += 60
            locals()['df_{}'.format(
                j)] = preprocessed_df.iloc[:, range(minute-60, minute)]
            locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)]
            # print(locals()['df_{}'.format(j)])
            locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)].T
            locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)].mean()
            locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)]
            # print(locals()['df_{}'.format(j)])
            hour_df['{}'.format(j)] = locals()['df_{}'.format(j)]
            # print(hour_df)
            pass
        pass

    hour_df = pd.concat([hour_df, tag_df], axis=1)
    # print(hour_df)

    #---------------- 한시간 단위로 되있는 데이터 후보지표별로 그래프 작성하여 확인 ----------------#

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

    #---------------- Clustering하기 위해 시계열 데이터만 남기기 ----------------#

    hour_df_ts = hour_df.iloc[:, range(24)]
    hour_df_tag = hour_df[['id', 'ymd', 'process',
                           'industry', 'week', 'Isholiday', 'season']]

    # display(hour_df_tag)

    # elbow 기법대신 수치로 보여주는 기법 사용하여 k-means 결과 뽑고 저장하기

    #---------------- silhouette 기법으로 알맞은 centroid 구하기(수치) ----------------#

    score_lst = []
    for i in range(2, 11):
        km = KMeans(n_clusters=i, init='k-means++', random_state=0)
        km.fit(hour_df_ts)
        cluster = km.predict(hour_df_ts)
        score = silhouette_score(hour_df_ts, cluster)
        score_lst.append(score)
        pass

    k_number = 0
    k_number_score = 0
    for i in range(len(score_lst)):
        if i != 0:

            if score_lst[i] >= k_number_score:
                k_number = i+1
                k_number_score = score_lst[i]
            else:
                pass
        else:
            pass
        pass

    print(score_lst)
    print(k_number)
    print(k_number_score)

    #----------------k_number을 사용하여 KMeanse Clustering 시행 ----------------#

    # KMeans 기법
    model = KMeans(n_clusters=k_number, init='k-means++',
                   random_state=0)  # 클러스터 수
    model.fit(hour_df_ts)
    Results = model.fit_predict(hour_df_ts)

    # hour_df_ts 데이터프레임과 hour_df 데이터프레임에 'class'열 생성해서 Clustering 결과 저장
    hour_df_ts['class'] = Results
    hour_df['class'] = Results
    display(hour_df_ts)

    # Clustering 결과 가시화
    grouped_data = hour_df_ts.groupby(['class']).mean()
    display(grouped_data)
    plt.title('Grouped Data')
    plt.xticks(range(1, 25))
    plt.ylabel('use')
    grouped_data_T = grouped_data.T

    for i in range(k_number):
        plt.plot(grouped_data_T[i], label='group {}'.format(i))
        pass

    # plt.legend()
    # plt.show()

    print(hour_df)

    #---------------- Clustering 결과 분석  ----------------#

    index_name = []
    index_total_number = []
    # 군집 개수만큼 리스트 생성
    for v in range(k_number):
        locals()['index_class_number_{}'.format(v)] = []
        pass

    # 세부지표들이 군집별로 몇%씩 포함되어있는지 확인하기 위해 데이터프레임 생성을 위한 리스트 만들기
    for i in candidate_index.keys():

        # 해당 후보지표를 보유한 데이터 추출
        for j in candidate_index[i]:
            index_df = hour_df[hour_df[i] == j]
            # 지표이름
            index_name.append(j)
            # 총 데이터에서 해당 지표가 포함되있는 데이터의 수
            index_total_number.append(len(index_df))

            # 군집별 해당 지표가 포함되어있는 데이터 수
            for k in range(k_number):
                locals()['index_class_number_{}'.format(k)].append(
                    len(index_df[index_df['class'] == k]))
                pass
            pass
        pass

    print(index_name)
    print(index_total_number)
    # print(index_class_number_0)

    # 위에서 구한 리스트들을 통해 군집별 지표가 포함되어있는 비율을 확인하기 위한 데이터프레임 생성
    index_number_in_class_df = pd.DataFrame(
        {'index': index_name, 'total_number': index_total_number})
    print(index_number_in_class_df)

    for k in range(k_number):
        index_number_in_class_df['class_{}'.format(k)] = locals()[
            'index_class_number_{}'.format(k)]
        pass

    # 비율 column 생성
    for k in range(k_number):
        index_number_in_class_df['class_{}_percent'.format(
            k)] = index_number_in_class_df['class_{}'.format(k)] / index_number_in_class_df['total_number']
        pass
    # 분모(데이터의 총 수)가 0일 경우 데이터 값이 nan값이 나오기 때문에, nan 값을 0으로 대체해주는 작업
    index_number_in_class_df = index_number_in_class_df.replace(np.nan, 0)

    print(index_number_in_class_df)

    # csv파일로 저장
    index_number_in_class_df.to_csv(
        'index_number_in_class.csv', index=False, encoding='cp949')

    #---------------- Clustering 결과 분석하여 연관있는 세부지표끼리 묶기  ----------------#

    relation_index_result_lst = []

    copy_candidate_index_checking = candidate_index_checking

    # candidate_index_checking 수치보다 높은 수치로 군집에 포함되어있는 세부지표 선별
    for k in range(k_number):
        index_checking_df = index_number_in_class_df[index_number_in_class_df['class_{}_percent'.format(
            k)] >= copy_candidate_index_checking]
        locals()['relation_class_{}'.format(
            k)] = index_checking_df['index'].tolist()
        print(locals()['relation_class_{}'.format(k)])

        for key in candidate_index.keys():
            # 지표내 항목중 관계있는 세부지표들을 모은 리스트
            locals()['relation_{}'.format(key)] = []
            # class별 연관 리스트
            temp_relation_class = locals()['relation_class_{}'.format(k)]

            # 만일, class별 연관 리스트의 항목이 같은 지표를 가지고 있다면, relation_{지표명}에 저장

            for sub_index in temp_relation_class:

                if sub_index in candidate_index[key]:

                    locals()['relation_{}'.format(key)].append(sub_index)
                else:
                    pass

            # 만일 relation_{지표명}의 개수가 2개 이상이면, relation_index_result_lst에 저장
            if len(locals()['relation_{}'.format(key)]) >= 2:
                relation_index_result_lst.append(
                    locals()['relation_{}'.format(key)])
            else:
                pass
            pass
        pass

    print(relation_index_result_lst)

    with open('relation_index_result_lst.pickle', 'wb') as f:
        pickle.dump(relation_index_result_lst, f)

    return relation_index_result_lst


# index_selecting(preprocessed_df, candidate_index,
#                 candidate_index_checking)
