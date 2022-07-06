import pandas as pd
import numpy as np
from IPython.display import display
from api_to_dataframe import api_to_dataframe
import datetime
import time
import os
import pickle
from information import industry_info, submetering_info, normalized_df_path, max_value_df


def normalization(submetering_info, normalized_df_path, max_value_df, industry_info):

    copy_max_value_df = max_value_df
    copy_submetering_info = submetering_info
    copy_industry_info = industry_info

    for indus in copy_industry_info:

        '''
        read pickle and modify the column
        '''

        if indus != 'desktop.ini':
            # take all the pickles in the folder(.\\pickle_folder\\industry_name) and combine them
            try:
                csv_path = ".\\pickle_folder\\{}".format(indus)
                folder_lst = os.listdir(csv_path)
                file_list_py = [
                    file for file in folder_lst if file.endswith('.pickle')]
                for individual in file_list_py:
                    try:
                        with open(csv_path + "\\" + individual, "rb") as file:
                            indiv_df = pickle.load(file)
                    except:
                        indiv_df = pd.DataFrame()
                    # if the data is not empty
                    if len(indiv_df) > 0:
                        indiv_df = indiv_df[[
                            'Measuring Time', 'Measuring ID', 'Active Power']]
                        indiv_df = indiv_df.reset_index(drop=True)
                        # modify column name
                        indiv_df.columns = ['time', 'id', 'use']
                        # add industry name column
                        indiv_df['industry'] = indus
                        indiv_df = indiv_df.astype({'time': str})
                        indiv_df = indiv_df.astype({'use': float})
                        # split the date
                        indiv_df['ymd'] = indiv_df['time'].str.slice(0, 10)
                        indiv_df[['year', 'month', 'day']] = pd.DataFrame(
                            indiv_df.ymd.str.split('-', 2).tolist())
                        indiv_df['sub_time'] = indiv_df['time'].str.slice(
                            11, 19)
                        indiv_df[['hour', 'min', 'second']] = pd.DataFrame(
                            indiv_df.sub_time.str.split(':', 2).tolist())
                        # add process column
                        merging_df = pd.merge(
                            indiv_df, copy_submetering_info, how='left', on='id')

                        '''
                        create column 'normalized_use' by normalizing 'use' with max value for id 
                        '''

                        normal_merging_df = pd.merge(
                            merging_df, copy_max_value_df, how='left', on='id')
                        normal_merging_df['normalized_use'] = normal_merging_df['use'] / \
                            normal_merging_df['max']
                        # fill the part with 'nan' that has a value of '0'
                        normal_merging_df['normalized_use'] = normal_merging_df['normalized_use'].replace(
                            np.nan, 0)
                        display(normal_merging_df)

                        '''
                        change seconds to minutes
                        '''

                        # average minute
                        normal_minute_df = normal_merging_df.groupby(
                            ['id', 'ymd', 'year', 'month', 'day', 'hour', 'min', 'process', 'industry', 'max'], as_index=False)['normalized_use'].mean()

                        '''
                        create dataframe using only 770 minutes(in 1440 minutes) or more per day 
                        '''

                        inputdata_ymd = []
                        inputdata_week = []
                        inputdata_industry = []
                        inputdata_id = []
                        inputdata_process = []
                        inputdata_id_use = []
                        inputdata_max = []

                        id_info = normal_minute_df['id'].unique()

                        # id
                        for k in id_info:
                            normal_minute_df_div = normal_minute_df[normal_minute_df['id'] == k]
                            date_lst = normal_minute_df_div.ymd.unique()
                            # date
                            for i in date_lst:
                                date_df = normal_minute_df_div[normal_minute_df_div['ymd'] == i].reset_index(
                                )
                                date_df['hour'] = date_df['hour'].astype(str)
                                date_df['min'] = date_df['min'].astype(str)
                                # if 'date_df' has 1,440 minutes
                                if len(date_df) == 1440:
                                    # enter series into list'inputdta_id_use' and single value for others
                                    inputdata_ymd.append(
                                        date_df.ymd.unique()[0])
                                    # set monday to 1
                                    inputdata_week.append(
                                        (datetime.datetime.strptime(i, '%Y-%m-%d').weekday())+1)
                                    inputdata_industry.append(
                                        date_df.industry.unique()[0])
                                    inputdata_id.append(date_df.id.unique()[0])
                                    inputdata_process.append(
                                        date_df.process.unique()[0])
                                    inputdata_max.append(
                                        date_df['max'].unique()[0])
                                    inputdata_id_use.append(
                                        date_df.normalized_use)
                                # if 'date_df' has more than 1,440 minutes
                                elif len(date_df['hour']) >= 770:
                                    # enter a single value into lists (except for the 'inputdata_id_use' list)
                                    inputdata_ymd.append(
                                        date_df.ymd.unique()[0])
                                    inputdata_week.append(
                                        (datetime.datetime.strptime(i, '%Y-%m-%d').weekday())+1)
                                    inputdata_industry.append(
                                        date_df.industry.unique()[0])
                                    inputdata_id.append(date_df.id.unique()[0])
                                    inputdata_process.append(
                                        date_df.process.unique()[0])
                                    inputdata_max.append(
                                        date_df['max'].unique()[0])
                                    # save minute data (use value)
                                    sub_inputdata_id_use = []
                                    # hour
                                    for h in range(24):
                                        h = str(h)
                                        h = h.zfill(2)
                                        # if the 'date_df' has 'h'
                                        if len(date_df[date_df['hour'] == h]) > 0:
                                            # minutes
                                            for m in range(60):
                                                m = str(m)
                                                # two digits number
                                                m = m.zfill(2)
                                                # if 'date_df' has 'm'
                                                if len(date_df[(date_df['hour'] == h) & (date_df['min'] == m)]) > 0:
                                                    # enter 'use' into list 'sub_inputdta_id_use'
                                                    sub_inputdata_id_use.extend(date_df[(date_df['hour'] == h) & (
                                                        date_df['min'] == m)]['normalized_use'].values)
                                                # if 'date_df' does not have 'm'
                                                else:
                                                    # enter 'Nan' into list 'sub_inputdta_id_use'
                                                    sub_inputdata_id_use.extend(
                                                        [np.nan])
                                                pass
                                        # if 'date_df' does not have 'h'
                                        elif len(date_df[date_df['hour'] == h]) == 0:
                                            # enter 'Nan' into 'sub_inputdata_id_use' 60 times
                                            for i in range(60):
                                                sub_inputdata_id_use.extend(
                                                    [np.nan])
                                                pass
                                    inputdata_id_use.append(
                                        sub_inputdata_id_use)
                                else:
                                    pass
                            else:
                                pass

                        '''
                        sort 'use' values by column
                        '''
                        columns_name = range(1440)
                        minute_data_lst = []
                        for j in inputdata_id_use:
                            df = np.array(j)
                            df = df.T
                            minute_data_lst.append(list(df))
                        id_normal_inputdata = pd.DataFrame(
                            minute_data_lst, columns=columns_name)
                        T_id_normal_inputdata = id_normal_inputdata.T

                        '''
                        fill unmeasured value
                        '''
                        T_id_normal_inputdata = T_id_normal_inputdata.fillna(
                            method='bfill')  # fill with values before
                        T_id_normal_inputdata = T_id_normal_inputdata.fillna(
                            method='ffill')  # fill with values after
                        id_normal_inputdata = T_id_normal_inputdata.T

                        # add others information
                        id_normal_inputdata['id'] = inputdata_id
                        id_normal_inputdata['process'] = inputdata_process
                        id_normal_inputdata['ymd'] = inputdata_ymd
                        id_normal_inputdata['industry'] = inputdata_industry
                        id_normal_inputdata['week'] = inputdata_week
                        id_normal_inputdata['max'] = inputdata_max

                        '''
                        remove outliers
                        '''
                        # remove negative values
                        # for i in range(1440):
                        #     id_normal_inputdata = id_normal_inputdata.loc[id_normal_inputdata[i] >= 0]
                        #     pass
                        # id_normal_inputdata = id_normal_inputdata.reset_index(
                        #     drop=True)
                        # #---------------- 이상치 제거(부하의 총 합이 0이면 제거) ----------------#
                        # id_info = id_normal_inputdata.id.unique()
                        # del_lst = []
                        # for i in id_info:
                        #     id_df = id_normal_inputdata[id_normal_inputdata['id'] == i]
                        #     ts_id_df = id_df.iloc[:, range(1440)]
                        #     print(ts_id_df.sum().sum())
                        #     if ts_id_df.sum().sum() == 0:
                        #         del_lst.append(i)
                        #     else:
                        #         pass

                        # for dl in del_lst:
                        #     id_normal_inputdata = id_normal_inputdata[id_normal_inputdata['id'] != dl]
                        #     pass

                        # if len(id_normal_inputdata) > 0:
                        #     '''
                        #     check holiday
                        #     '''
                        #     # 공휴일 API에 넣을 형태 year+month column 추가
                        #     id_normal_inputdata['ymd'] = id_normal_inputdata['ymd'].astype(
                        #         str)
                        #     id_normal_inputdata['year'] = id_normal_inputdata['ymd'].str.slice(
                        #         0, 4)
                        #     id_normal_inputdata['month'] = id_normal_inputdata['ymd'].str.slice(
                        #         5, 7)
                        #     id_normal_inputdata['API_Year_Month'] = id_normal_inputdata['year'].astype(
                        #         str)+id_normal_inputdata['month'].astype(str)

                        #     # 공휴일 API 적용 - 공휴일 정보 생성
                        #     date_info = id_normal_inputdata.ymd.unique()
                        #     API_Input_Data_df = pd.DataFrame({
                        #         'Year_Month': id_normal_inputdata.API_Year_Month.unique()  # 연도, 월을 저장하여 API class에 입력
                        #     }, columns=['Year_Month'])
                        #     API_Input_Data_df['year'] = API_Input_Data_df['Year_Month'].str.slice(
                        #         0, 4)
                        #     API_Input_Data_df['month'] = API_Input_Data_df['Year_Month'].str.slice(
                        #         5, 7)

                        #     # 날짜와 휴일 유/무(1/0) 저장
                        #     temp_h = []
                        #     temp_is_h = []
                        #     for i in range(len(API_Input_Data_df)):
                        #         # api를 사용하여 holiday인 날짜만 있는 데이터 추리기
                        #         temp_h_df = api_to_dataframe.get_api_data(
                        #             API_Input_Data_df['year'][i], API_Input_Data_df['month'][i].zfill(2))
                        #         for j in range(len(temp_h_df)):
                        #             # holiday인 날짜 저장
                        #             temp_h.append(temp_h_df.values[j])
                        #             # holiday임을 표시
                        #             temp_is_h.append('h')
                        #         pass
                        #     temp_df = pd.DataFrame({'ymd': date_info})
                        #     temp_h_df = pd.DataFrame(
                        #         {'time': temp_h, 'Isholiday': temp_is_h})

                        #     # holiday df의 time을 ymd 형태로 만들어서 저장하기
                        #     temp_h_df['time'] = temp_h_df['time'].astype(str)
                        #     temp_h_df['year'] = temp_h_df['time'].str.slice(
                        #         2, 6)
                        #     temp_h_df['month'] = temp_h_df['time'].str.slice(
                        #         6, 8)
                        #     temp_h_df['day'] = temp_h_df['time'].str.slice(
                        #         8, 10)
                        #     temp_h_df['ymd'] = temp_h_df['year']+'-' + \
                        #         temp_h_df['month']+'-'+temp_h_df['day']

                        #     # id_normal_inputdata에 있는 날짜를 중복없이 정리한 temp_df와 연도, 월, holiday 유무로 이루어진 temp_h_df 합치기
                        #     holiday_df = pd.merge(
                        #         temp_df, temp_h_df[['ymd', 'Isholiday']], how='left', on='ymd')
                        #     holiday_df = holiday_df.replace(np.nan, 'nh')

                        #     # 기존 dataframe에 api로부터 뽑은 holiday정보 합치기
                        #     copy_id_normal_inputdata = id_normal_inputdata
                        #     preprocessed_df = pd.merge(
                        #         copy_id_normal_inputdata, holiday_df, how='left', on=['ymd'])

                        #     # 6,7(토,일)의 holiday를 1로 표시
                        #     preprocessed_df.loc[preprocessed_df['week']
                        #                         == 6, 'Isholiday'] = 'h'
                        #     preprocessed_df.loc[preprocessed_df['week']
                        #                         == 7, 'Isholiday'] = 'h'

                        #     display(preprocessed_df)

                        #     #---------------- season tag 추가 ----------------#

                        #     # 데이터 타입 변경
                        #     preprocessed_df = preprocessed_df.astype(
                        #         {'month': float})

                        #     # season cloumn 추가하기
                        #     preprocessed_df.loc[(preprocessed_df['month'] < 3) | (
                        #         preprocessed_df['month'] > 10), 'season'] = 'winter'
                        #     preprocessed_df.loc[(preprocessed_df['month'] > 5) & (
                        #         preprocessed_df['month'] < 9), 'season'] = 'summer'
                        #     preprocessed_df.loc[(preprocessed_df['month'] == 9) | (
                        #         preprocessed_df['month'] == 10), 'season'] = 'fall'
                        #     preprocessed_df.loc[(preprocessed_df['month'] > 2) & (
                        #         preprocessed_df['month'] < 6), 'season'] = 'spring'

                        #     #tag_df = preprocessed_df[['ymd', 'Isholiday', 'id', 'process', 'industry', 'season', 'max']]

                        #     preprocessed_df = preprocessed_df.drop(
                        #         ['year', 'month', 'API_Year_Month'], axis=1)

                        # save as pickle file
                        df = open("..\\{}normalized_df_{}_{}".format(
                            normalized_df_path, indus, individual), "wb")
                        pickle.dump(id_normal_inputdata, df)
                        df.close()

                        # save as csv file
                        id_normal_inputdata.to_csv("..\\normalized_csv_file\\normalized_df_{}_{}.csv".format(
                            indus, individual), index=False, encoding='utf-8-sig')
                    else:
                        print('{} data is not exist'.format(individual))
            except:
                pass
        else:
            pass


# normalization(submetering_info, normalized_df_path,
#               max_value_df, industry_info)
