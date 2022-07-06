import pandas as pd
import numpy as np
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from IPython.display import display, HTML
from sqlalchemy import create_engine, engine
from information import candidate_index

df = pd.read_csv('..//process_normalized_index_clustering_df_0701_0930.csv')


def association(df):

    # 데이터타입을string으로 바꾸기
    df['Isholiday'] = df['Isholiday'].astype(str)
    df['class'] = df['class'].astype(str)
    #####

    display(df)

    # 해당 군집에 해당 지표가 몇개 있는지 적혀있는 'n' 칼럼 삭제
    df = df.drop(columns=['n'], axis=1)

    # 데이터프레임 값을 list 형태로 변환
    data = df.values.tolist()

    # Assiciation 하기 위한 input 데이터 입력

    te = TransactionEncoder()
    te_ary = te.fit(data).transform(data)
    te_df = pd.DataFrame(te_ary, columns=te.columns_)

    # 최소 지지도, 최소 신뢰도 설정
    rule = apriori(te_df, min_support=0.05, use_colnames=True)
    data1 = association_rules(
        rule, metric='confidence', min_threshold=0.3, support_only=False)

    # 읽어올 내용 지정
    data2 = data1.iloc[:, [0, 1, 4, 5, 6]]

    antecedents = []
    consequents = []
    support = []
    confidence = []
    lift = []

    # 연관분석 결과에서 consequents가 군집인 경우 추출
    for i, row in data2.iterrows():
        print(list(row.consequents))
        if (list(row.consequents)[0] in ["0", "1", "2", "3", "4", "5"]) == 1:
            # 결과 중복을 막기 위해 antecedents와 consequents가 하나로 이루어진 결과 추출
            if (len(list(row.antecedents)) == 1) and (len(list(row.consequents)) == 1):
                antecedents.append(list(row.antecedents)[0])
                consequents.append(list(row.consequents)[0])
                support.append(row.support)
                confidence.append(row.confidence)
                lift.append(row.lift)
            else:
                pass
        else:
            pass
        pass

    data3 = pd.DataFrame({'antecedents': antecedents, 'consequents': consequents,
                         'support': support, 'confidence': confidence, 'lift': lift})
    print(data3)

    process_info = df.process.unique()
    consequents_info = data3.consequents.unique()

    # 출력값 넣을 리스트 생성
    output_lst = []

    # consequents에 같은 key를 가진 항목이 있으면 data set으로 사용
    for i in consequents_info:
        len(data3[data3['consequents'] == i]) > 0
        season_lst = []
        industry_lst = []
        isholiday_lst = []
        process_lst = []

        for j in data3[data3['consequents'] == i]['antecedents']:
            print(j)
            if j in candidate_index['season']:
                season_lst.append(j)
            elif j in candidate_index['industry']:
                industry_lst.append(j)
            elif j in candidate_index['Isholiday']:
                isholiday_lst.append(j)
            elif j in process_info:
                process_lst.append(j)
            else:
                pass

        print(season_lst)
        print(industry_lst)
        print(isholiday_lst)
        print(process_lst)

        if len(season_lst) > 1:
            output_lst.append(season_lst)
        else:
            pass

        if len(industry_lst) > 1:
            output_lst.append(industry_lst)
        else:
            pass

        if len(isholiday_lst) > 1:
            output_lst.append(isholiday_lst)

        if len(process_lst) > 1:
            output_lst.append(process_lst)

        else:
            pass

    print(output_lst)

    # csv파일로 저장
    data3.to_csv(
        '..//process_normalized_association_df_0701_0930.csv', index=False)

    return output_lst


association(df)
