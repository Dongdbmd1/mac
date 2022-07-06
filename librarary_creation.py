
from information import industry_info, submetering_info, candidate_index, choosing_index, candidate_index_checking, normalized_df_path, potential_lst, host_info, user_info, password_info, max_value_df
from data_pickling import data_pickling
from normalization import normalization
from preprocessing import preprocessing
from index_selecting import index_selecting
from section_clustering import section_clustering
from ramping_potential import ramping_potential
from building_database import devide_rows, database_creation, cbl_table_creation, cbl_table_data_insert, cbl_table_data_delete, potential_table_creation, potential_table_data_insert, potential_table_data_delete
from new_file_save import new_file_save
from max_value_save import max_value_save

# SFTP로 데이터 받아오기
new_file_save()

# 데이터 받아와서 pickle로 저장 및 'max_value.csv' 업데이트
data_pickling()

# pickle단위로 전처리 진행 후 pickle로 저장
normalization(submetering_info, normalized_df_path,
              max_value_df, industry_info)

# 전처리된 pickle 합치기
preprocessed_df = preprocessing(normalized_df_path)

# 임시 #
# preprocessed_df = pd.read_csv(
#     '..//preprocessed_df.csv', index_col=False)
# merging_indus_name = '|'.join(industry_info)
# preprocessed_df = preprocessed_df[preprocessed_df['industry'].str.contains(
#     merging_indus_name)]

# 관련있는 세부지표끼리 묶어 리스트 형태로 반환
relation_index_result_lst = index_selecting(
    preprocessed_df, candidate_index, candidate_index_checking, industry_info)


# # 임시 #
# with open('..//relation_index_result_lst.pickle', 'rb') as f:
#     relation_index_result_lst = pickle.load(f)

# 관련있는 세부지표 반영하여 군집화
clusterized_df = section_clustering(
    preprocessed_df, relation_index_result_lst, candidate_index, choosing_index)


# # 임시 #
# clusterized_df = pd.read_csv('..//clusterized_df.csv', encoding='cp949')
# clusterized_df = clusterized_df.replace('h_탈지/고주파', 'h_탈지_고주파')
# clusterized_df = clusterized_df.replace('nh_탈지/고주파', 'h_탈지_고주파')


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


# 잠재량 측정 지표 생성
RUL_df, RDL_df, RU_df, RD_df = ramping_potential(clusterized_df)
print(RUL_df)
print(RDL_df)
print(RU_df)
print(RD_df)


# #데이터베이스 생성
# database_creation('flexibleDR', host_info, user_info, password_info)

#clusterized_df = pd.read_csv('..//clusterized_df.csv', encoding="CP949")
# RUL_df = pd.read_csv('..//RUL.csv')
# RDL_df = pd.read_csv('..//RDL.csv')
# RU_df = pd.read_csv('..//RU.csv')
# RD_df = pd.read_csv('..//RD.csv')

# RU_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# RUL_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# RD_df.drop(['Unnamed: 0'], axis=1, inplace=True)
# RDL_df.drop(['Unnamed: 0'], axis=1, inplace=True)


# pk 생성
clusterized_df['PK'] = clusterized_df.index

# 행 1000개씩 나눠담기
clusterized_df_lst = devide_rows(clusterized_df)


# 데이터 베이스 "기준부하" 테이블 생성
cbl_table_creation(clusterized_df_lst, 'flexibleDR',
                   host_info, user_info, password_info)
# 데이터 베이스 "잠재량" 테이블 생성
potential_table_creation(potential_lst, 'flexibleDR',
                         host_info, user_info, password_info)

# #데이터 베이스 "기준부하" 테이블 내 데이터 삭제
cbl_table_data_delete(clusterized_df_lst, 'flexibleDR',
                      host_info, user_info, password_info)
# 데이터 베이스 "잠재량" 테이블 내 데이터 삭제
potential_table_data_delete(
    potential_lst, 'flexibleDR', host_info, user_info, password_info)

# 데이터베이스 "기준부하" 테이블 내 데이터 삽입
cbl_table_data_insert(clusterized_df_lst, 'flexibleDR',
                      host_info, user_info, password_info)
# 데이터베이스 "잠재량" 테이블 내 데이터 삽입
potential_table_data_insert(potential_lst, 'flexibleDR', host_info,
                            user_info, password_info, RU_df, RUL_df, RD_df, RDL_df)
