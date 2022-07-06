import pandas as pd
import numpy as np
from IPython.display import display, HTML
from os import system      
from IPython.display import Image
from sqlalchemy import create_engine, engine


# #---------------- csv 데이터 불러오기 ----------------#
# # #데이터 읽어오기 
# clusterized_df = pd.read_csv('clusterized_df.csv')

# clusterized_df = clusterized_df.reset_index(drop=True)
# #df.drop(['Unnamed: 1'], axis=1, inplace = True)

# clusterized_df = clusterized_df.replace([np.inf,-np.inf], None)
# clusterized_df = clusterized_df.dropna(axis=0)


def ramping_potential(clusterized_df):
    #---------------- 지표셋별 데이터프레임 분리 ----------------#
    #지표셋 정보 저장
    indexset_info = clusterized_df.indexset.unique()

    div_indexset_df_lst = []
    div_indexset_name_lst  =[]

    #지표셋별 데이터 프레임 분리하여 div_indexset_df_lst에 저장, indexset이름은 indexset_name_lst에 저장
    for i in indexset_info:
        div_indexset_df = clusterized_df[clusterized_df['indexset']==i]
        div_indexset_df_lst.append(div_indexset_df)
        div_indexset_name_lst.append(i)
        pass

    #---------------- 지표셋내 그룹별 cbl 및 잠재량측정지표 생성----------------#
    #지표셋 이름 저장 
    indexset_name_lst = []
    #지표셋내 그룹 이름 저장
    group_name_lst = []
    cbl_lst = []
    #수요 증가 가능량
    RU_lst = []
    #수요 감소 가능량
    RD_lst = []
    #수요 증가 상한량
    RUL_lst = []
    #수요 감소 하한량 
    RDL_lst = []


    for j in range(len(div_indexset_df_lst)):
        ts_indexset_df = div_indexset_df_lst[j]
        #시계열 데이터만 남기기 
        ts_indexset_df = ts_indexset_df.iloc[:, range(1440)]
        #시계열 데이터에 'class'열 추가해주기
        ts_indexset_df['class'] = div_indexset_df_lst[j]['class']
        print(ts_indexset_df)
        #지표셋내 class정보 저장 
        class_info = ts_indexset_df['class'].unique()
        #class별 평균값 구하기 
        for cl in class_info:

            group_df = ts_indexset_df[ts_indexset_df['class']==cl]
            group_df_mean = group_df.groupby(['class']).mean()

            cbl_df = group_df_mean

            group_df = group_df.drop(['class'], axis = 1)
            group_df = group_df.reset_index(drop=True)
            cbl_df = cbl_df.reset_index(drop=True)

            print(group_df)
            print(cbl_df)

            ##RU,RD구하기####

            group_df.columns = range(1440)
            cbl_df.columns = range(1440)

            front_df = group_df[range(0,1440)]
            back_df = group_df[range(1,1440)]
            #back_df에 0시 추가
            back_df[[0]] = group_df[[0]]
            #back_df의 column을 front_df와 일치시키기 
            back_df.columns = range(0,1440)
            # print(front_df)
            # print(back_df)



            RU = (back_df-front_df).max()
            RD = (back_df - front_df).min()
            print(RU)
            print(RD)



            
            max_df = group_df.max()
            min_df = group_df.min()

            RUL = max_df - cbl_df
            RDP = min_df - cbl_df

            indexset_name_lst.append(div_indexset_name_lst[j])
            group_name_lst.append(cl)
            cbl_lst.append(cbl_df.iloc[0])
            RUL_lst.append(RUL)
            RDL_lst.append(RDP)
            RU_lst.append(RU)
            RD_lst.append(RD)



    RUL_df = pd.DataFrame()
    RDL_df = pd.DataFrame()
    RU_df = pd.DataFrame()
    RD_df = pd.DataFrame()

    for i in range(len(RUL_lst)):
        RUL_df[i] = RUL_lst[i].iloc[0]
        RDL_df[i] = RDL_lst[i].iloc[0]
        RU_df[i] = RU_lst[i]
        RD_df[i] = RD_lst[i]
        pass

    RUL_df = RUL_df.T
    RDL_df = RDL_df.T
    RU_df = RU_df.T
    RD_df = RD_df.T

    # print(RUL_df)
    # print(RDL_df)
    # print(RU_df)
    # print(RD_df)


    RUL_df['indexset'] = indexset_name_lst 
    RDL_df['indexset'] = indexset_name_lst 
    RU_df['indexset'] = indexset_name_lst 
    RD_df['indexset'] = indexset_name_lst 

    RUL_df['class'] = group_name_lst
    RDL_df['class'] = group_name_lst
    RU_df['class'] = group_name_lst
    RD_df['class'] = group_name_lst


    print(RUL_df)
    print(RDL_df)
    print(RU_df)
    print(RD_df)

    #---------------- 데이터프레임 조건부 치환 (UP은 - 값 나올 때 0으로, DOWN은 +값 나올 때 0으로) ----------------#

    for i in range(1440):
        RUL_df[i] = RUL_df[i].apply(lambda x : 0 if x<0 else x )
        RDL_df[i] = RDL_df[i].apply(lambda x : 0 if x>0 else x )
        RU_df[i] = RU_df[i].apply(lambda x : 0 if x<0 else x )
        RD_df[i] = RD_df[i].apply(lambda x : 0 if x>0 else x )
        pass


    print(RUL_df)
    print(RDL_df)
    print(RU_df)
    print(RD_df)

    RUL_df.reset_index()
    RDL_df.reset_index()
    RU_df.reset_index()
    RD_df.reset_index()
    

    RU_df['PK'] = RU_df.index
    RD_df['PK'] = RD_df.index
    RUL_df['PK'] = RUL_df.index
    RDL_df['PK'] = RDL_df.index
    #---------------- csv파일로 잠재량측정지표 저장 ----------------#

    RUL_df.to_csv('RUL.csv', index = False)
    RDL_df.to_csv('RDL.csv', index = False)
    RU_df.to_csv('RU.csv', index = False)
    RD_df.to_csv('RD.csv', index = False)

    return RUL_df, RDL_df, RU_df, RD_df