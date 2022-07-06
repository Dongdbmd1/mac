import pandas as pd
import numpy as np
from IPython.display import display, HTML
import os
import pickle
import pymysql
from sqlalchemy import create_engine
import openpyxl
from openpyxl import load_workbook
import pyexcel as p
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'] 
credentials = ServiceAccountCredentials.from_json_keyfile_name('Key.json', scope) 
gc = gspread.authorize(credentials) 

spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1odBYQ5nk83hlHfSxTHM20ghXS3rOKbpz6l2iVRMxyu8/edit?usp=sharing'

#스프레스시트 문서 가져오기
doc = gc.open_by_url(spreadsheet_url)

#시트 선택하기
worksheet_survey = doc.worksheet('2.설비정보')
mol_worksheet_survey = doc.worksheet('2.설비정보(국토부)')

#특정 셀 출력 (예제)
# cell_data = worksheet_1.acell('A1').value
# print(cell_data)

# 리스트 값을 활용하여 데이터프레임 작성(일반)
#특정 열 출력(일반)
survey_info_number = worksheet_survey.col_values(8)
survey_info_process = worksheet_survey.col_values(11)

# 데이터프레임 생성
survey_info_df = pd.DataFrame({survey_info_number[0]: survey_info_number[1:], survey_info_process[0]: survey_info_process[1:]})
# 필요없는 정보 삭제(공정에 값이 안들어가있는 데이터)
drop_index = survey_info_df[survey_info_df[survey_info_process[0]] == '' ].index

survey_info_df = survey_info_df.drop(drop_index)
survey_info_df = survey_info_df.reset_index(drop=True)

print(survey_info_df)

# 리스트 값을 활용하여 데이터프레임 작성(국토부)
#특정 열 출력(국토부)
mol_survey_info_number = mol_worksheet_survey.col_values(4)
mol_survey_info_process = mol_worksheet_survey.col_values(5)

# 데이터프레임 생성
mol_survey_info_df = pd.DataFrame({mol_survey_info_number[0]: mol_survey_info_number[1:], mol_survey_info_process[0]: mol_survey_info_process[1:]})
# 필요없는 정보 삭제(공정에 값이 안들어가있는 데이터)
drop_index = mol_survey_info_df[mol_survey_info_df[mol_survey_info_process[0]] == '' ].index

mol_survey_info_df = mol_survey_info_df.drop(drop_index)
mol_survey_info_df = mol_survey_info_df.reset_index(drop=True)

print(mol_survey_info_df)

total_survey_info_df = pd.concat([survey_info_df, mol_survey_info_df], ignore_index = True)

print(total_survey_info_df)

total_survey_info_df.to_csv('survey_info.csv', index=False, encoding= "cp949")