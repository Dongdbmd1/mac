import pandas as pd
import numpy as np
from IPython.display import display
# 데이터를 1) 훈련, 2) 검증 데이터셋으로 분리하기 위해 사용합니다.
from sklearn.model_selection import train_test_split
# 모델의 성능을 평가하기 위해 사용합니다.
from sklearn.svm import SVC

from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import LogisticRegression

# #데이터 읽어오기
Clustering_df = pd.read_csv(
    'clusterized_df.csv', encoding='CP949')


try:
    Clustering_df.drop(['Unnamed: 0'], axis=1, inplace=True)
except:
    pass

df = Clustering_df.reset_index(drop=True)


df = df.replace([np.inf, -np.inf], None)
df = df.dropna(axis=0)

print(df)

# prcess에 '/'가 들어가있으면 _로 변경
df = df.replace('h_탈지/고주파', 'h_탈지_고주파')
df = df.replace('nh_탈지/고주파', 'h_탈지_고주파')
################10분 단위로 바꿔볼까 ######################
minute = 0
tag_df = df[['indexset', 'class']]
tag_df['class'] = pd.to_numeric(df['class'])
#ts_lst = []

new_df = pd.DataFrame()


while minute != 1440:

    for i in range(144):

        j = i+1
        minute += 10
        locals()['df_{}'.format(j)] = df.iloc[:, range(minute-10, minute)]
        locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)]
        # print(locals()['df_{}'.format(j)])
        locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)].T
        locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)].mean()
        locals()['df_{}'.format(j)] = locals()['df_{}'.format(j)]
        # print(locals()['df_{}'.format(j)])

        new_df['{}'.format(j)] = locals()['df_{}'.format(j)]
        # print(new_df)
#       ts_lst.append(locals()['df_{}'.format(j)])

    pass

# minute_df = pd.DataFrame(ts_lst, columns = range(1,25))
# print(minute_df)

df = pd.concat([new_df, tag_df], axis=1)
print(df)


def classification_error(df):

    index_division_df = df

    indexset_info = index_division_df['indexset'].unique()
    # 결과파일 만들 list 생성
    mathod_lst = []
    error_lst = []
    class_lst = []
    index_name_lst = []

    NB_lst = []
    NB_accuracy_lst = []
    SVM_lst = []
    SVM_accuracy_lst = []
    LR_lst = []
    LR_accuracy_lst = []

    for nm in indexset_info:

        df = index_division_df[index_division_df['indexset'] == nm]
        display(df)

        ############## feature와 target 정의하기 ##############
        columns = ['{}'.format(i) for i in range(144)]

        x = df.iloc[:, range(144)]
        y = df['class']

        # feature와 target 합치기
        df = pd.concat([x, y], axis=1)

        display(df)

        class_mean_df = df.groupby(['class']).mean()
        # display(class_mean_df)

        # ############## Class별 평가 ##############

        # for cl, row in class_mean_df.iterrows():
        #     display(cl)
        #     y_true = df[df['class'] == cl][columns]
        #     y_pred = pd.DataFrame([row], columns = columns)

        #     y_pred_copy = pd.DataFrame([row], columns = columns)

        #     for i in range(len(df[df['class'] == cl].index)-1):
        #         y_pred = y_pred.append(y_pred_copy)
        #     #y_pred = pd.concat([y_pred, pd.DataFrame([row], columns = columns)], axis=0)
        #         pass
        #     # display(y_pred)
        #     y_true = y_true.reset_index(drop=True)
        #     y_pred = y_pred.reset_index(drop=True)
        #     # display(len(y_true.index))
        #     # display(len(y_pred.index))

        #     # Clustering_df = pd.concat(all_df, axis=0)
        #     # display(Clustering_df)

        ############### SVM ###############

        if len(x) > 3:
            x_train, x_test, y_train, y_test = train_test_split(
                x, y, test_size=0.2)

            locals()['{}_x_y_train_df'.format(nm)] = pd.concat(
                [x_train, y_train], axis=1)
            locals()['{}_x_y_test_df'.format(nm)] = pd.concat(
                [x_test, y_test], axis=1)

            if len(y_train.unique()) != 1:

                # SVM
                svm_model = SVC(kernel='linear', C=100)
                svm_model = SVC(kernel='linear')
                svm_model.fit(x_train, y_train)

                # 테스트
                final_result = svm_model.predict(x_test)
                accuracy = "{:.2f}".format(np.mean(final_result == y_test))
                SVM_lst.append('Support Vector machine')
                SVM_accuracy_lst.append(accuracy)

                # #NB
                nb = GaussianNB()
                nb.fit(x_train, y_train)
                # 테스트
                final_result = nb.predict(x_test)
                accuracy = "{:.2f}".format(np.mean(final_result == y_test))
                NB_lst.append('Naive Bayes')
                NB_accuracy_lst.append(accuracy)

                # logistic regression
                lr = LogisticRegression()
                lr.fit(x_train, y_train)
                # 테스트
                final_result = lr.predict(x_test)
                accuracy = "{:.2f}".format(np.mean(final_result == y_test))
                LR_lst.append('logistic regression')
                LR_accuracy_lst.append(accuracy)
                index_name_lst.append('{}'.format(nm))
                locals()['{}_x_y_train_df'.format(nm)].to_csv(
                    '.\\train_df\\{}_x_y_train_df.csv'.format(nm))
                locals()['{}_x_y_test_df'.format(nm)].to_csv(
                    '.\\test_df\\{}_x_y_test_df.csv'.format(nm))

            else:
                pass

        else:
            pass

    evaluation_df = pd.DataFrame(
        {'index': index_name_lst, 'mathod1': NB_lst, 'value1': NB_accuracy_lst, 'mathod2': SVM_lst, 'value2': SVM_accuracy_lst, 'mathod3': LR_lst, 'value3': LR_accuracy_lst, })

    display(evaluation_df)

    evaluation_df.to_csv(
        'ten_minuite_Classification_svm_evaluation.csv', encoding='CP949')


classification_error(df)
