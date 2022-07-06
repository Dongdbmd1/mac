import pandas as pd
import numpy as np
import os

# 공장 이름
try:
    industry_info = os.listdir("..\\pickle_folder")
except:
    industry_info = []


# 공정 정보
submetering_info = pd.read_csv('survey_info.csv', encoding='cp949')
submetering_info = submetering_info[['관리번호', '설비정보']]
submetering_info = submetering_info.rename(
    columns={'관리번호': 'id', '설비정보': 'process'})


industry_info = pd.read_csv('..\\industry_info.csv',
                            index_col=False, encoding='cp949')

# indus_lst = list(industry_info['industry'])

# industry_info = ['중외제약', '부천주물', '대창 후문공장', '율촌2공장']

indus_lst = industry_info


# 인덱스로 쓰일 후보 지표
candidate_index = {'season': ['spring', 'summer', 'fall', 'winter'],
                   'industry': indus_lst,
                   'Isholiday': ['h', 'nh'],
                   'process': submetering_info.process.unique()}

# candidate_index = {'season': ['spring', 'summer', 'fall', 'winter'],
#                    'industry': indus_lst,
#                    'Isholiday': ['h', 'nh']}

# print(candidate_index['process'])

# 인덱스로 쓰일 지표((휴일유무, 공정),(휴일유무, 계절, 공장),(휴일유무,계절,공정, 공장),(휴일유무, 계절, 공정) 중 선택)
# choosing_index = ['season', 'industry', 'Isholiday']
# choosing_index = ['Isholiday', 'process']
choosing_index = ['season', 'Isholiday']
# choosing_index = ['season','industry','Isholiday','process']
# choosing_index = ['season', 'process', 'Isholiday']

sftp_info = {'host': 'ess.gridwiz.com', 'port': 59501,
             'username': 'fxdr', 'password': 'flex1234!@#'}


# 후보지표분석을 할 때 지표가 특정 군집에 몇(0~1) 이상 포함되어 있을 때 의미가 있다고 볼지 정하기
candidate_index_checking = 0.60


# id별 전력사용 max값 미리 알고있어야함 (왜냐하면 용량때문에 id별 정규화를 10일치씩 하는데, 그 안에서의 id의 전력사용 max값을 사용하는데는 오류가 있기 때문이다)
max_value_df = pd.read_csv('max_value.csv')


# 데이터베이스 테이블 (63개)
fdrproject_cbl_db_0 = ['0_start_library_0', '199_start_library_0', '398_start_library_0', '597_start_library_0',
                       '796_start_library_0', '995_start_library_0', '1194_start_library_0', '1393_start_library_0', 'tag_0']
fdrproject_cbl_db_1 = ['0_start_library_1', '199_start_library_1', '398_start_library_1', '597_start_library_1',
                       '796_start_library_1', '995_start_library_1', '1194_start_library_1', '1393_start_library_1', 'tag_1']
fdrproject_cbl_db_2 = ['0_start_library_2', '199_start_library_2', '398_start_library_2', '597_start_library_2',
                       '796_start_library_2', '995_start_library_2', '1194_start_library_2', '1393_start_library_2', 'tag_2']
fdrproject_cbl_db_3 = ['0_start_library_3', '199_start_library_3', '398_start_library_3', '597_start_library_3',
                       '796_start_library_3', '995_start_library_3', '1194_start_library_3', '1393_start_library_3', 'tag_3']
fdrproject_cbl_db_4 = ['0_start_library_4', '199_start_library_4', '398_start_library_4', '597_start_library_4',
                       '796_start_library_4', '995_start_library_4', '1194_start_library_4', '1393_start_library_4', 'tag_4']
fdrproject_ru_db = ['ru_0_start_library', 'ru_199_start_library', 'ru_398_start_library', 'ru_597_start_library',
                    'ru_796_start_library', 'ru_995_start_library', 'ru_1194_start_library', 'ru_1393_start_library', 'ru_tag']
fdrproject_rd_db = ['rd_0_start_library', 'rd_199_start_library', 'rd_398_start_library', 'rd_597_start_library',
                    'rd_796_start_library', 'rd_995_start_library', 'rd_1194_start_library', 'rd_1393_start_library', 'rd_tag']
fdrproject_rup_db = ['rup_0_start_library', 'rup_199_start_library', 'rup_398_start_library', 'rup_597_start_library',
                     'rup_796_start_library', 'rup_995_start_library', 'rup_1194_start_library', 'rup_1393_start_library', 'rup_tag']
fdrproject_rdp_db = ['rdp_0_start_library', 'rdp_199_start_library', 'rdp_398_start_library', 'rdp_597_start_library',
                     'rdp_796_start_library', 'rdp_995_start_library', 'rdp_1194_start_library', 'rdp_1393_start_library', 'rdp_tag']

# print(submetering_info)

# 전처리된 데이터가 저장되는 폴더 경로
normalized_df_path = "..\\normalized_file\\"


# 잠재량 측정지표
potential_lst = ['RU', 'RD', 'RUL', 'RDL']

# 데이터베이스 정보
host_info = "127.0.0.1"
user_info = "root"
password_info = "0901"
