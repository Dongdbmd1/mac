# _max_value_save.py 돌리기 
# 오늘 날짜로 저장된 폴더의 데이터들 다 읽어오기


import pandas as pd
import numpy as np
from IPython.display import display, HTML
from sklearn.cluster import KMeans
import os
import pickle
# import pymysql
from sqlalchemy import create_engine
#import csv
# from _new_file_save import new_file_save


# # 새로운 데이터 불러와서 저장하기 
# new_file_save()

######파일이 들어있는 경로(폴더명: 'create_folder_name.csv'파일에 기재되어있음)
# 폴더명 가져오기
rdr = pd.read_csv('create_folder_name.csv', header=None)
f_name = rdr.iloc[-1][0]

print(f_name)

#해당 폴더 내 폴더 리스트 저장 
sub_folder_lst = os.listdir("./{}".format(f_name))

# max 폴더 불러오기 
max_value_df = pd.read_csv('max_value.csv')
#데이터 합치기(리스트에 담고 나중에 합치기)
save_file_lst = []

# sub_folder(i = 예. 회사명_부하_연도_월) 리스트 돌기 
for i in sub_folder_lst:
    if i != "desktop.ini":


        ### 해당 경로, 폴더에 들어가서 안에 있는 csv 파일들 목록 불러오기

        csv_path = "./{}/{}/".format(f_name, i)
        folder_lst = os.listdir(csv_path)
        file_list_py = [file for file in folder_lst if file.endswith('.csv')]
        

        #해당 폴더에 파일이 있을 때만 실행
        if len(file_list_py) != 0: 
            for j in file_list_py:
                data = pd.read_csv(csv_path + j)
                data = data[['Measuring Time','Measuring ID','Active Power']]
                data = data.dropna()

                if len(data) != 0: 
                    save_file_lst.append(data)
                    temp_id = str(i).split('_',2)[1]
                    temp_max = data['Active Power'].max()

                    # max_value.csv 에 해당 부하가 있을때
                    if temp_id  in max_value_df.loc[:,'id'].to_list():
                        max = int(max_value_df.loc[max_value_df['id'] == temp_id ]['max'])

                        #temp_max와 max 값 비교  
                        if int(temp_max) > int(max):
                            max_value_df.loc[((max_value_df['id']==temp_id ), 'max')] = temp_max
                        else:
                            pass
                    # max_value.csv 에 해당 부하가 없을때 
                    elif temp_id  not in max_value_df.loc[:,'id'].to_list():
                        max_value_df = max_value_df.append({'id':temp_id , 'max': temp_max}, ignore_index = True)
                        pass
                else:
                    pass


        else:
            pass

    else:
        pass
pass          


save_file = pd.DataFrame()
for sf in save_file_lst:
    save_file = pd.concat([save_file, sf])
    pass
save_file = save_file.reset_index(drop=True)
max_value_df = max_value_df[max_value_df['max']!=0]
# max_valu_df 업데이트
max_value_df.to_csv('max_value.csv', index = False)
# 새로 받은 데이터 피클 저장(업데이트 기간 날짜)
save_file_name = open("new_data_{}.pickle".format(f_name), "wb")
pickle.dump(save_file, save_file_name)
save_file_name.close()







# # save_file 전처리


# copy_max_value_df = max_value_df
# copy_submetering_info = submetering_info
# copy_industry_info = industry_info

# print(copy_industry_info)
# #copy_save date만큼 for문 돌리기 
# for indus in copy_industry_info:
#     #---------------- 데이터 읽어와서 필요한 column 수정 및 추가하기(데이터프레임 칼럼: 연,월,일,시,분,초,아이디,전력사용량,공장명) ----------------#

#     if indus != 'desktop.ini' :
#         #### pickle_folder의 해당 공장명 폴더 안에 있는 pickle 다 가져와서 합치기 
#         try:
#             # csv파일명 가져오기 
#             csv_path = ".\\pickle_folder\\{}".format(indus)
#             folder_lst = os.listdir(csv_path)
#             file_list_py = [file for file in folder_lst if file.endswith('.pickle')]


#         ############# #file_list_py 1개씩(부하 1씩)
            
#             count = 0 
#             for individual in file_list_py:
#                 count += 1 
            
#                 # with open(csv_path + "/" + individual, "rb") as file:
#                 #     save_file = pickle.load(file)
#                 # 데이터가 비워있지 않을 경우 
            
#                 try:
#                     with open(csv_path + "\\" + individual, "rb") as file:
#                         save_file = pickle.load(file)

#                 except:
#                     save_file = pd.DataFrame() 

#                 if len(save_file) > 0:
#                     save_file = save_file[['Measuring Time','Measuring ID','Active Power']]
#                     save_file = save_file.reset_index(drop=True)
#                     save_file.columns = ['time', 'id', 'use']
#                     save_file['industry'] = indus
                    
#                     save_file = save_file.astype({'time': str})
#                     save_file = save_file.astype({'use': float})

#                     # 날짜 쪼개기 
#                     save_file['ymd'] = save_file['time'].str.slice(0, 10) 
#                     save_file[['year', 'month', 'day']] = pd.DataFrame(save_file.ymd.str.split('-',2).tolist())
#                     save_file['sub_time'] = save_file['time'].str.slice(11, 19)
#                     save_file[['hour','min','second']] = pd.DataFrame(save_file.sub_time.str.split(':',2).tolist())



#                     #---------------- 파일들 합치고 id에 따른 공정 추가하기(데이터프레임 칼럼: 연,월,일,시,분,초,아이디,전력사용량,공장명, 공정명) ----------------#

#                     merging_df = pd.merge(save_file, copy_submetering_info, how = 'left', on= 'id')

#                     print(merging_df)


#                     #---------------- 부하별 max로 정규화된 use 칼럼 생성 및 값 적용 ----------------#

#                     normal_merging_df = pd.merge(merging_df, copy_max_value_df, how='left', on='id') 
#                     normal_merging_df['normalized_use'] = normal_merging_df['use']/normal_merging_df['max']
#                     normal_merging_df['normalized_use'] = normal_merging_df['normalized_use'].replace(np.nan,0) # 0의 값을 갖는 부분을 nan으로 채우기
#                     display(normal_merging_df)

#                     normal_merging_df['max']= normal_merging_df['max'].replace(np.nan, 0, regex=True)
#                     #---------------- 초단위 데이터를 분단위 데이터로 바꾸기 ----------------#



#                     # 분 단위 평균
#                     normal_minute_df = normal_merging_df.groupby(['id', 'ymd', 'year', 'month', 'day', 'hour', 'min','process', 'industry', 'max'], as_index=False)['normalized_use'].mean()
                    
#                     print(normal_minute_df)

#                     # csv파일로 저장 
#                     #normal_minute_df.to_csv('normal_minute_df_{}.csv'.format(individual), index=False)

#                     #---------------- 하루 770분 이상으로 있는 데이터만 사용하여 데이터프레임 생성 ----------------#

#                     # 하루 1440분이 다 있는 데이터만 쓰기 위한 작업  
#                     inputdata_ymd = []
#                     inputdata_week = []

#                     inputdata_industry = []
#                     inputdata_id = []
#                     inputdata_process = []

#                     inputdata_id_use = []
#                     inputdata_max = []

#                     id_info = normal_minute_df['id'].unique()


#                     # 770분 이상으로 있는 데이터 선별
#                     for k in id_info:
#                         normal_minute_df_div = normal_minute_df[normal_minute_df['id'] == k]
#                         date_lst = normal_minute_df_div.ymd.unique()
#                         for i in date_lst:
#                             date_df = normal_minute_df_div[normal_minute_df_div['ymd'] == i].reset_index()
#                             date_df['hour'] = date_df['hour'].astype(str)
#                             date_df['min'] = date_df['min'].astype(str)      
#                             # 만약 분단위 1440개 다 있으면
#                             if len(date_df['hour']) == 1440:
#                                 # 데이터프레임을 만들 리스트에 추가

#                                 inputdata_ymd.append(date_df.ymd.unique()[0])
#                                 # 월요일이 1이 되도록 
#                                 inputdata_week.append((datetime.datetime.strptime(i,'%Y-%m-%d').weekday())+1)
#                                 inputdata_industry.append(date_df.industry.unique()[0])
#                                 inputdata_id.append(date_df.id.unique()[0])
#                                 inputdata_process.append(date_df.process.unique()[0])
#                                 inputdata_max.append(date_df['max'].unique()[0])
#                                 inputdata_id_use.append(date_df.normalized_use)


#                             # 만약 분단위 770개 이상 있으면
#                             elif len(date_df['hour']) >= 770:
#                                 # 데이터프레임을 만들 리스트에 추가
#                                 inputdata_ymd.append(date_df.ymd.unique()[0])
#                                 inputdata_week.append((datetime.datetime.strptime(i,'%Y-%m-%d').weekday())+1)
#                                 inputdata_industry.append(date_df.industry.unique()[0])
#                                 inputdata_id.append(date_df.id.unique()[0])
#                                 inputdata_process.append(date_df.process.unique()[0])
#                                 inputdata_max.append(date_df['max'].unique()[0])

#                                 # 분단위로 수요 전력 값 저장(없는 '분'이 있을 수도 있으니까)
#                                 sub_inputdata_id_use = []

#                                 # 하루 24시간을 0부터 돌 때
#                                 for h in range(24):
#                                     h = str(h)
#                                     h = h.zfill(2)
#                                     # 해당 시간이 있다면 
#                                     if len(date_df[date_df['hour'] == h]) > 0 :
#                                         # 1시간 60분을 돌 때 
#                                         for m in range(60):
#                                             m = str(m)
#                                             # 정수부분 2자리 수로 채우기 
#                                             m = m.zfill(2)    
#                                             # 해당 분이 있다면
#                                             if len(date_df[(date_df['hour']==h)&(date_df['min'] == m)]) > 0:
#                                                 #전력사용값 채우기 
#                                                 sub_inputdata_id_use.extend(date_df[(date_df['hour']==h)&(date_df['min']== m)]['normalized_use'].values)
#                                             # 해당 분이 없다면 
#                                             else:
#                                                 # Nan값 채우기
#                                                 sub_inputdata_id_use.extend([np.nan])
#                                             pass
#                                     # 해당 시간이 없다면 
#                                     elif len(date_df[date_df['hour'] == h]) == 0:
#                                         # 해당 시간을 Nan값으로 채우기 
#                                         for i in range(60):
#                                             sub_inputdata_id_use.extend([np.nan])
#                                             pass                 
#                                 inputdata_id_use.append(sub_inputdata_id_use)
#                             else:
#                                 pass
#                         else:
#                             pass    

#                     #---------------- 분단위 전력 사용량 열 정렬 ----------------#

#                     # 군집화에 쓰이기 위해선 시계열 데이터가 열정렬 되어야 함 
#                     columns_name = range(1440)

#                     minute_data_lst = []

#                     # 시계열 데이터 열정렬로 바꾸기
#                     for j in inputdata_id_use:
#                         df = np.array(j)
#                         df = df.T
#                         minute_data_lst.append(list(df))


#                     # 군집화에 쓰일 inputdata형태 완성 
#                     id_normal_inputdata = pd.DataFrame(minute_data_lst, columns = columns_name)
#                     T_id_normal_inputdata = id_normal_inputdata.T


#                     #---------------- 미측정 값에 값 채워넣기 (id_normal)----------------#

#                     # 미측정 값에 값 채워넣기 (앞, 뒤 시간)
#                     # id_normal_inputdata.use = id_normal_inputdata.use.replace(0, np.nan) # 0의 값을 갖는 부분을 nan으로 채우기
#                     T_id_normal_inputdata = T_id_normal_inputdata.fillna(method = 'bfill') # nan 값을 갖는 부분을 앞에 값을 가져와서 채우기
#                     T_id_normal_inputdata= T_id_normal_inputdata.fillna(method = 'ffill') # nan 값을 갖는 부분을 뒤에 값을 가져와서 채우기
#                     id_normal_inputdata = T_id_normal_inputdata.T

#                     # 시계열 데이터 외 정보 추가 
#                     id_normal_inputdata['id'] = inputdata_id
#                     id_normal_inputdata['process'] = inputdata_process
#                     id_normal_inputdata['ymd'] = inputdata_ymd
#                     id_normal_inputdata['industry'] = inputdata_industry
#                     id_normal_inputdata['week'] = inputdata_week
#                     id_normal_inputdata['max'] = inputdata_max

#                     display(id_normal_inputdata)

#                     #---------------- 이상치 제거(마이너스값 데이터 제거) ----------------#
#                     for i in range(1440):
#                         id_normal_inputdata = id_normal_inputdata.loc[id_normal_inputdata[i]>=0]
#                         pass

#                     id_normal_inputdata = id_normal_inputdata.reset_index(drop=True)


#                     if len(id_normal_inputdata) > 0:
#                         #---------------- 공휴일 체킹 ----------------#

#                         # 공휴일 API에 넣을 형태 year+month column 추가 
#                         id_normal_inputdata['ymd'] = id_normal_inputdata['ymd'].astype(str)
#                         id_normal_inputdata['year'] = id_normal_inputdata['ymd'].str.slice(0, 4)
#                         id_normal_inputdata['month'] = id_normal_inputdata['ymd'].str.slice(5, 7)
#                         id_normal_inputdata['API_Year_Month'] = id_normal_inputdata['year'].astype(str)+id_normal_inputdata['month'].astype(str)


#                         # 공휴일 API 적용 - 공휴일 정보 생성 
#                         date_info = id_normal_inputdata.ymd.unique()
#                         API_Input_Data_df = pd.DataFrame({
#                             'Year_Month' : id_normal_inputdata.API_Year_Month.unique() # 연도, 월을 저장하여 API class에 입력
#                         },columns=['Year_Month'])
#                         API_Input_Data_df['year'] = API_Input_Data_df['Year_Month'].str.slice(0, 4)
#                         API_Input_Data_df['month'] = API_Input_Data_df['Year_Month'].str.slice(5, 7)

#                         # 날짜와 휴일 유/무(1/0) 저장 
#                         temp_h = []
#                         temp_is_h = []
#                         for i in range(len(API_Input_Data_df)):
#                             # api를 사용하여 holiday인 날짜만 있는 데이터 추리기 
#                             temp_h_df = api_to_dataframe.get_api_data(API_Input_Data_df['year'][i], API_Input_Data_df['month'][i].zfill(2))
#                             for j in range(len(temp_h_df)):
#                                 # holiday인 날짜 저장 
#                                 temp_h.append(temp_h_df.values[j])
#                                 # holiday임을 표시 
#                                 temp_is_h.append('h')
#                             pass
#                         temp_df = pd.DataFrame({'ymd' : date_info})
#                         temp_h_df = pd.DataFrame({'time' : temp_h,'Isholiday' : temp_is_h})

#                         # holiday df의 time을 ymd 형태로 만들어서 저장하기  
#                         temp_h_df['time'] = temp_h_df['time'].astype(str)
#                         temp_h_df['year'] = temp_h_df['time'].str.slice(2, 6)
#                         temp_h_df['month'] = temp_h_df['time'].str.slice(6, 8)
#                         temp_h_df['day'] = temp_h_df['time'].str.slice(8, 10)
#                         temp_h_df['ymd'] = temp_h_df['year']+'-'+temp_h_df['month']+'-'+temp_h_df['day']

#                         #id_normal_inputdata에 있는 날짜를 중복없이 정리한 temp_df와 연도, 월, holiday 유무로 이루어진 temp_h_df 합치기
#                         holiday_df = pd.merge(temp_df, temp_h_df[['ymd', 'Isholiday']], how='left', on='ymd')
#                         holiday_df = holiday_df.replace(np.nan, 'nh')

#                         # 기존 dataframe에 api로부터 뽑은 holiday정보 합치기  
#                         copy_id_normal_inputdata = id_normal_inputdata
#                         preprocessed_df = pd.merge(copy_id_normal_inputdata, holiday_df, how='left', on=['ymd'])

#                         # 6,7(토,일)의 holiday를 1로 표시
#                         preprocessed_df.loc[preprocessed_df['week']==6, 'Isholiday'] = 'h'
#                         preprocessed_df.loc[preprocessed_df['week']==7, 'Isholiday'] = 'h'

#                         display(preprocessed_df) 

#                         #---------------- season tag 추가 ----------------#

#                         #데이터 타입 변경
#                         preprocessed_df = preprocessed_df.astype({'month' : float})

#                         #season cloumn 추가하기 
#                         preprocessed_df.loc[(preprocessed_df['month'] < 3) | (preprocessed_df['month'] > 10), 'season'] = 'winter'
#                         preprocessed_df.loc[(preprocessed_df['month'] > 5) & (preprocessed_df['month'] < 9), 'season'] = 'summer'
#                         preprocessed_df.loc[(preprocessed_df['month'] ==9) | (preprocessed_df['month'] == 10), 'season'] = 'fall'
#                         preprocessed_df.loc[(preprocessed_df['month'] > 2) & (preprocessed_df['month'] < 6), 'season'] = 'spring'

#                         #tag_df = preprocessed_df[['ymd', 'Isholiday', 'id', 'process', 'industry', 'season', 'max']]

#                         preprocessed_df = preprocessed_df.drop(['year','month','API_Year_Month'], axis = 1)

                        
#                         # pickle로 저장 
#                         df = open("{}preprocessing_df_{}_{}".format(preprocessed_df_path, indus, individual), "wb")
#                         pickle.dump(preprocessed_df, df)
#                         df.close()
                        
#                         # csv파일로 저장
#                         preprocessed_df.to_csv(".\\preprocessed_csv_file\\preprocessed_df_{}_{}.csv".format(indus,individual), index=False, encoding='utf-8-sig')
                    
#                     else: 
#                         print('{} 데이터가 없습니다'.format(individual))
#                 else:
#                     print('{} 데이터가 없습니다'.format(individual))
            
#         except:
#             pass
#     else:
#         pass



# save_file indexset 생성
# _classification 돌리기
# 오차율 확인하기 
# information에 기재한 오차율보다 오차율이 높으면 라이브러리 재생성하기 