from lib2to3.pgen2.pgen import DFAState
import pandas as pd
from IPython.display import display
import os
import pickle
from information import normalized_df_path
from api_to_dataframe import api_to_dataframe
import pandas as pd
import numpy as np


def preprocessing(normalized_df_path):

    copy_normalized_df_path = normalized_df_path

    # import all pickle files in that path
    path = "{}".format(copy_normalized_df_path)
    file_list = os.listdir(path)
    file_list_py = [file for file in file_list if file.endswith('.pickle')]

    # merging data
    df = pd.DataFrame()
    for i in file_list_py:
        pk = open(path + i, "rb")
        data = pickle.load(pk)
        pk.close()

        df = pd.concat([df, data])

    df = df.reset_index(drop=True)

    display(df)

    '''
    remove outliers
    '''
    # remove negative values
    for i in range(1440):
        df = df.loc[df[i] >= 0]
        pass
    df = df.reset_index(
        drop=True)
    # remove if total load is 0
    id_info = df.id.unique()
    del_lst = []
    for i in id_info:
        id_df = df[df['id'] == i]
        ts_id_df = id_df.iloc[:, range(1440)]
        print(ts_id_df.sum().sum())
        if ts_id_df.sum().sum() == 0:
            del_lst.append(i)
        else:
            pass

    for dl in del_lst:
        df = df[df['id'] != dl]
        pass

    '''
    check holiday
    '''

    # creat a form to put in 'holiday API'
    df['ymd'] = df['ymd'].astype(
        str)
    df['year'] = df['ymd'].str.slice(
        0, 4)
    df['month'] = df['ymd'].str.slice(
        5, 7)
    df['API_Year_Month'] = df['year'].astype(
        str)+df['month'].astype(str)

    # apply 'holiday API'
    date_info = df.ymd.unique()
    API_Input_Data_df = pd.DataFrame({
        # enter year, month information in API class
        'Year_Month': df.API_Year_Month.unique()
    }, columns=['Year_Month'])
    API_Input_Data_df['year'] = API_Input_Data_df['Year_Month'].str.slice(
        0, 4)
    API_Input_Data_df['month'] = API_Input_Data_df['Year_Month'].str.slice(
        5, 7)

    # save holiday True/False(h,nh) of date
    temp_h = []
    temp_is_h = []
    for i in range(len(API_Input_Data_df)):
        # get only date data is a holiday
        temp_h_df = api_to_dataframe.get_api_data(
            API_Input_Data_df['year'][i], API_Input_Data_df['month'][i].zfill(2))
        for j in range(len(temp_h_df)):
            # save holiday date
            temp_h.append(temp_h_df.values[j])
            # holiday indication
            temp_is_h.append('h')
        pass
    temp_df = pd.DataFrame({'ymd': date_info})
    temp_h_df = pd.DataFrame(
        {'time': temp_h, 'Isholiday': temp_is_h})

    # create 'ymd' column through 'time'
    temp_h_df['time'] = temp_h_df['time'].astype(str)
    temp_h_df['year'] = temp_h_df['time'].str.slice(
        2, 6)
    temp_h_df['month'] = temp_h_df['time'].str.slice(
        6, 8)
    temp_h_df['day'] = temp_h_df['time'].str.slice(
        8, 10)
    temp_h_df['ymd'] = temp_h_df['year']+'-' + \
        temp_h_df['month']+'-'+temp_h_df['day']

    # create 'holiday_df' by merging 'temp_df', 'temp_h_df'
    holiday_df = pd.merge(
        temp_df, temp_h_df[['ymd', 'Isholiday']], how='left', on='ymd')
    holiday_df = holiday_df.replace(np.nan, 'nh')

    # create 'preprocessed_df' by merging 'df' and 'holiday_df'
    copy_df = df
    preprocessed_df = pd.merge(
        copy_df, holiday_df, how='left', on=['ymd'])

    # marrk 6,7(sat,sun)'s 'holiday'column as 'h'
    preprocessed_df.loc[preprocessed_df['week']
                        == 6, 'Isholiday'] = 'h'
    preprocessed_df.loc[preprocessed_df['week']
                        == 7, 'Isholiday'] = 'h'

    display(preprocessed_df)

    '''
    add season tag
    '''
    # 데이터 타입 변경
    preprocessed_df = preprocessed_df.astype(
        {'month': float})

    # season cloumn 추가하기
    preprocessed_df.loc[(preprocessed_df['month'] < 3) | (
        preprocessed_df['month'] > 10), 'season'] = 'winter'
    preprocessed_df.loc[(preprocessed_df['month'] > 5) & (
        preprocessed_df['month'] < 9), 'season'] = 'summer'
    preprocessed_df.loc[(preprocessed_df['month'] == 9) | (
        preprocessed_df['month'] == 10), 'season'] = 'fall'
    preprocessed_df.loc[(preprocessed_df['month'] > 2) & (
        preprocessed_df['month'] < 6), 'season'] = 'spring'

    #tag_df = preprocessed_df[['ymd', 'Isholiday', 'id', 'process', 'industry', 'season', 'max']]

    preprocessed_df = preprocessed_df.drop(
        ['year', 'month', 'API_Year_Month'], axis=1)
    '''
    save preprocessed data as pickle and csv file 
    '''
    # save as pickle file
    preprocessed_df = open("..\\preprocessed_df.pickle", "wb")
    pickle.dump(df, preprocessed_df)
    preprocessed_df.close()
    # save as csv file
    df.to_csv('..\\preprocessed_df.csv')
    preprocessed_df = df

    return(preprocessed_df)
