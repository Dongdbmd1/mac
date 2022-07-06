import pandas as pd
import numpy as np
from IPython.display import display, HTML
import os
import pymysql
import pickle
# from _information import potential_lst


# #---------------- csv파일 불러오기 ----------------#

# # 기준부하
# clusterized_df = pd.read_csv('clusterized_df.csv')
# clusterized_df['PK'] = clusterized_df.index

# display(clusterized_df)

# # 잠재량
# RU_df = pd.read_csv('RU.csv', index_col=0)
# RD_df = pd.read_csv('RD.csv', index_col=0)
# RUL_df = pd.read_csv('RUL.csv', index_col=0)
# RDL_df = pd.read_csv('RDL.csv', index_col=0)


# # 데이터베이스 정보
# host_info = "127.0.0.1"
# user_info = "root"
# password_info = "0901"

####### 기준부하 데이터베이스 구축 #########

# 행 1000개로씩 나눠담는 함수
def devide_rows(clusterized_df):

    j = 0
    clusterized_df_lst = []

    for i in range(round(len(clusterized_df)/1000) + 1):

        # 1000개씩 묶어서 데이터프레임 나누기
        locals()['clusterized_df_{}'.format(i)] = clusterized_df[(clusterized_df['PK'] >= int(
            '{}'.format(j))) & (clusterized_df['PK'] < int('{}'.format(j))+1000)]
        j += 1000
        print(locals()['clusterized_df_{}'.format(i)])
        # 1000개씩 묶어서 나눈 데이터프레임 저장
        clusterized_df_lst.append(locals()['clusterized_df_{}'.format(i)])
        pass

    return(clusterized_df_lst)

# 데이터 베이스 생성 함수


def database_creation(database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), charset="utf8")
    curs = conn.cursor()

    # 라는 데이터베이스 생성
    sql = "CREATE DATABASE {}".format(database_name)

    curs.execute(sql)
    conn.commit()
    conn.close()

# 데이터 베이스 "기준부하" 테이블 생성 함수


def cbl_table_creation(clusterized_df_lst, database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    for kkk in range(len(clusterized_df_lst)):

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블 생성 sql 저장할 리스트
        sql_lst = []

        while i != 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:
                # i부터 시작하는 kkk번째 테이블
                sql = "CREATE TABLE `{}_start_library_{}` (".format(
                    str(i), str(kkk))

                for j in range(200):
                    if j != 199:
                        sql = sql + \
                            ("`{}` float(10,6) NOT NULL,".format(str(i)))
                        i += 1
                    else:
                        sql = sql + "`PK` int(11) NOT NULL );"
                        sql_lst.append(sql)

            else:
                sql = "CREATE TABLE `{}_start_library_{}` (".format(
                    str(i), str(kkk))
                # 남은 시계열 데이터가 199보다 적으면, i가 1439에 도달할 때까지 해당 알고리즘을 돌려 테이블 생성
                while i < 1440:
                    sql = sql + ("`{}` float(10,6) NOT NULL,".format(str(i)))
                    i += 1

                    if i == 1440:
                        sql = sql + "`PK` INT(11) NOT NULL );"
                        sql_lst.append(sql)

        # 생성된 sql문들을 DB로 excute
        for i in range(len(sql_lst)):
            try:
                sql = sql_lst[i]
                curs.execute(sql)
                conn.commit()
            except:
                pass

        pass

    
        # tag 정보를 담는 테이블도 생성
        sql = '''CREATE TABLE `tag_{}` (
            `indexset` VARCHAR(50) NOT NULL,
            `class` VARCHAR(10) NOT NULL,
            `PK` int(11) NOT NULL); '''.format(kkk)
        try:
            curs.execute(sql)
            conn.commit()
        except:
            pass

    conn.close()

# 데이터 베이스 "기준부하" 테이블 내 데이터 삭제 함수


def cbl_table_data_delete(clusterized_df_lst, database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    for kkk in range(len(clusterized_df_lst)):

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블명 저장 리스트
        table_lst = []
        while i <= 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:

                # 테이블명 저장
                table_lst.append(
                    '{}_start_library_{}'.format(str(i), str(kkk)))

                i += 199

            else:

                table_lst.append(
                    '{}_start_library_{}'.format(str(i), str(kkk)))
                i += 199

        table_lst.append("tag_{}".format(kkk))

        # 삭제
        for i in table_lst:
            sql = 'TRUNCATE `{}`;'.format(i)
            curs.execute(sql)
            conn.commit()
            pass

    conn.close()

# 데이터베이스 "기준부하" 테이블 내 데이터 삽입 함수


def cbl_table_data_insert(clusterized_df_lst, database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    for kkk in range(len(clusterized_df_lst)):

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블 column명 저장할 리스트
        col_lst = []
        # column 개수대로 %S를 생성하여 저장할 리스트
        per_s_lst = []
        # 시계열데이터 삽입시 인덱스로 사용(테이블별 열 개수 저장)
        len_num_lst = []
        # 테이블명 저장 리스트
        table_lst = []
        while i != 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:

                # i부터 시작하는 kkk번째 테이블
                col = "("
                per_s = ""
                len_num = 0

                # 테이블명 저장
                table_lst.append(
                    '{}_start_library_{}'.format(str(i), str(kkk)))

                for j in range(200):
                    if j != 199:

                        col = col + "`{}`,".format(str(i))
                        per_s = per_s + "%s,"

                        len_num += 1
                        i += 1

                    else:

                        col = col + "`PK`)"
                        col_lst.append(col)
                        per_s = per_s + "%s"
                        per_s_lst.append(per_s)
                        len_num_lst.append(len_num)

            else:

                col = "("
                per_s = ""
                len_num = 0
                table_lst.append(
                    '{}_start_library_{}'.format(str(i), str(kkk)))

                # 남은 시계열 데이터가 199보다 적으면, i가 1439에 도달할 때까지 해당 알고리즘을 돌려 테이블 생성
                while i < 1440:

                    col = col + "`{}`,".format(str(i))
                    per_s = per_s + "%s,"
                    len_num += 1
                    i += 1

                    if i == 1440:

                        col = col + "`PK`)"
                        col_lst.append(col)
                        per_s = per_s + "%s"
                        per_s_lst.append(per_s)
                        len_num_lst.append(len_num)

        # 데이터프레임 시계열데이터만 뽑아서 밑에 돌리기

        clusterized_df = clusterized_df_lst[kkk]

        timeseries_data = clusterized_df.iloc[:, range(1440)]
        timeseries_data['PK'] = clusterized_df['PK']

        print(timeseries_data)

        # 시계열 데이터 데이터 삽입

        index_info = 0

        for i in range(len(col_lst)):
            sql = """INSERT INTO `{}`.`{}` 
                {} 
                VALUES ({});""".format(database_name, table_lst[i], col_lst[i], per_s_lst[i])

            for index, row in timeseries_data.iterrows():
                k_lst = []

                for j in range((len_num_lst[i])):

                    if j != len_num_lst[i] - 1:
                        k = str(row.iloc[index_info + j])
                        k_lst.append(k)

                    elif j == len_num_lst[i] - 1:
                        k = str(row.iloc[index_info + j])
                        k_lst.append(k)

                        k = str(row.loc['PK'])
                        k_lst.append(k)

                        a = tuple(k_lst)

                        curs.execute(sql, a)
                        conn.commit()
            index_info += 199

        # 데이터베이스에 tag 데이터 삽입

        tag_data = clusterized_df[['indexset', 'class', 'PK']]

        # 데이터 삽입

        sql = """INSERT INTO `{}`.`tag_{}` 
                (`indexset`,`class`,`PK`)
                VALUES (%s, %s, %s);""".format(database_name, str(kkk))

        for index, row in tag_data.iterrows():

            a = (row.iloc[0], row.iloc[1], row.iloc[2])
            curs.execute(sql, a)
        conn.commit()
    curs.close()


####### 잠재량 데이터베이스 구축 #########


# 데이터 베이스 "잠재량" 테이블 생성 함수
def potential_table_creation(potential_lst, database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    for kkk in potential_lst:

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블 생성 sql 저장할 리스트
        sql_lst = []

        while i != 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:
                # i부터 시작하는 kkk번째 테이블
                sql = "CREATE TABLE `{}_{}_start_library` (".format(
                    str(kkk), str(i))

                for j in range(200):
                    if j != 199:
                        sql = sql + \
                            ("`{}` float(10,6) NOT NULL,".format(str(i)))
                        i += 1
                    else:
                        sql = sql + "`PK` int(11) NOT NULL );"
                        sql_lst.append(sql)

            else:
                sql = "CREATE TABLE `{}_{}_start_library` (".format(
                    str(kkk), str(i))
                # 남은 시계열 데이터가 199보다 적으면, i가 1439에 도달할 때까지 해당 알고리즘을 돌려 테이블 생성
                while i < 1440:
                    sql = sql + ("`{}` float(10,6) NOT NULL,".format(str(i)))
                    i += 1
                    if i == 1440:
                        sql = sql + "`PK` INT(11) NOT NULL );"
                        sql_lst.append(sql)

        # 생성된 sql문들을 DB로 excute
        for i in range(len(sql_lst)):
            sql = sql_lst[i]
            try:
                curs.execute(sql)
                conn.commit()
            except:
                pass

        pass

        # tag 정보를 담는 테이블도 생성
        sql = '''CREATE TABLE `{}_tag` (
            `indexset` VARCHAR(50) NOT NULL,
            `class` VARCHAR(10) NOT NULL,
            `PK` int(11) NOT NULL); '''.format(kkk)
        try:
            curs.execute(sql)
            conn.commit()
        except:
            pass

    conn.close()


# 데이터 베이스 "잠재량" 테이블 내 데이터 삭제 함수
def potential_table_data_delete(potential_lst, database_name, host_info, user_info, password_info):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    for kkk in potential_lst:

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블명 저장 리스트
        table_lst = []
        while i <= 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:

                # 테이블명 저장
                table_lst.append(
                    '{}_{}_start_library'.format(str(kkk), str(i)))

                i += 199

            else:

                table_lst.append(
                    '{}_{}_start_library'.format(str(kkk), str(i)))
                i += 199

        table_lst.append("{}_tag".format(kkk))

        # 삭제
        for i in table_lst:
            sql = 'TRUNCATE `{}`;'.format(i)
            curs.execute(sql)
            conn.commit()
            pass

    conn.close()


# 데이터베이스 "잠재량" 테이블 내 데이터 삽입 함수
def potential_table_data_insert(potential_lst, database_name, host_info, user_info, password_info, RU_df, RUL_df, RD_df, RDL_df):
    conn = pymysql.connect(host="{}".format(host_info), user="{}".format(
        user_info), password="{}".format(password_info), db="{}".format(database_name), charset="utf8")
    curs = conn.cursor()

    RU_df = RU_df
    RUL_df = RUL_df
    RD_df = RD_df
    RDL_df = RDL_df

    for kkk in potential_lst:

        # 시계열 데이터 column 분리 (200개씩)
        i = 0
        # 테이블 column명 저장할 리스트
        col_lst = []
        # column 개수대로 %S를 생성하여 저장할 리스트
        per_s_lst = []
        # 시계열데이터 삽입시 인덱스로 사용(테이블별 열 개수 저장)
        len_num_lst = []
        # 테이블명 저장 리스트
        table_lst = []
        while i != 1440:
            # 분단위 데이터가 총 1440개인데, 0부터 카운트하기 때문에 1439를 기준으로 둠. PK(1개) + 시계열 데이터(199개)를 한 테이블에 넣음
            if 1439 - i > 198:

                # i부터 시작하는 kkk번째 테이블
                col = "("
                per_s = ""
                len_num = 0

                # 테이블명 저장
                table_lst.append(
                    '{}_{}_start_library'.format(str(kkk), str(i)))

                for j in range(200):
                    if j != 199:

                        col = col + "`{}`,".format(str(i))
                        per_s = per_s + "%s,"

                        len_num += 1
                        i += 1

                    else:

                        col = col + "`PK`)"
                        col_lst.append(col)
                        per_s = per_s + "%s"
                        per_s_lst.append(per_s)
                        len_num_lst.append(len_num)

            else:

                col = "("
                per_s = ""
                len_num = 0
                table_lst.append(
                    '{}_{}_start_library'.format(str(kkk), str(i)))

                # 남은 시계열 데이터가 199보다 적으면, i가 1439에 도달할 때까지 해당 알고리즘을 돌려 테이블 생성
                while i < 1440:

                    col = col + "`{}`,".format(str(i))
                    per_s = per_s + "%s,"
                    len_num += 1
                    i += 1

                    if i == 1440:

                        col = col + "`PK`)"
                        col_lst.append(col)
                        per_s = per_s + "%s"
                        per_s_lst.append(per_s)
                        len_num_lst.append(len_num)

        print(locals()['{}_df'.format(kkk)])

        # 데이터프레임 시계열데이터만 뽑아서 밑에 돌리기
        insert_df = locals()['{}_df'.format(kkk)]

        timeseries_data = insert_df.iloc[:, range(1440)]
        timeseries_data['PK'] = insert_df['PK']

        # 시계열 데이터 데이터 삽입

        index_info = 0

        for i in range(len(col_lst)):
            sql = """INSERT INTO `{}`.`{}` 
                {} 
                VALUES ({});""".format(database_name, table_lst[i], col_lst[i], per_s_lst[i])

            for index, row in timeseries_data.iterrows():
                k_lst = []

                for j in range((len_num_lst[i])):

                    if j != len_num_lst[i] - 1:
                        k = str(row.iloc[index_info + j])
                        k_lst.append(k)

                    elif j == len_num_lst[i] - 1:
                        k = str(row.iloc[index_info + j])
                        k_lst.append(k)

                        k = str(row.loc['PK'])
                        k_lst.append(k)

                        a = tuple(k_lst)

                        curs.execute(sql, a)
                        conn.commit()
            index_info += 199

        # 데이터베이스에 tag 데이터 삽입

        tag_data = locals()['{}_df'.format(kkk)]

        tag_data = tag_data[['indexset', 'class', 'PK']]

        # 데이터 삽입

        sql = """INSERT INTO `{}`.`{}_tag` 
                (`indexset`,`class`,`PK`)
                VALUES (%s, %s, %s);""".format(database_name, str(kkk))

        for index, row in tag_data.iterrows():

            a = (row.iloc[0], row.iloc[1], row.iloc[2])
            curs.execute(sql, a)
        conn.commit()
    curs.close()
