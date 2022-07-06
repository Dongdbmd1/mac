import pandas as pd
import os
import pickle


def max_value_save_new(df):
    # read 'max_value.csv' file
    max_value_df = pd.read_csv('max_value.csv')
    # run if the input data(df) is not empty
    if len(df) > 0:
        temp_id = df['Measuring ID'][0]
        temp_max = df['Active Power'].max()
        # when the 'max_value.csv'file has its submetering data id
        if temp_id in max_value_df.loc[:, 'id'].to_list():
            max = int(
                max_value_df.loc[max_value_df['id'] == temp_id]['max'])
            # cmparison of the 'max'value with the 'temp_max(previous max)' value
            if int(temp_max) > int(max):
                max_value_df.loc[(
                    (max_value_df['id'] == temp_id), 'max')] = temp_max
            else:
                pass
        # when the 'max_value.csv'file doens not have its submetering data id
        elif temp_id not in max_value_df.loc[:, 'id'].to_list():

            max_value_df = max_value_df.append(
                {'id': temp_id, 'max': temp_max}, ignore_index=True)
            pass
    # delete rows with max value of '0'
    max_value_df = max_value_df[max_value_df['max'] != 0]
    # save as file 'max_value.csv'
    max_value_df.to_csv('max_value.csv', index=False)
    max_value_df = 0


def max_value_save_whole():
    # path where pickle is stored(industry name)
    folder_lst = os.listdir(".\\pickle_folder")
    # read 'max_value.csv' file
    max_value_df = pd.read_csv('max_value.csv')
    for folder in folder_lst:
        if folder != "desktop.ini":
            path = ".\\pickle_folder" + "\\{}".format(folder)
            # pickle file name list
            pickle_file_lst = os.listdir(path)
            # open one pickle at a time
            for i in range(len(pickle_file_lst)):
                if pickle_file_lst[i] != "desktop.ini":
                    file = path + "\\" + pickle_file_lst[i]
                    with open(file, 'rb') as pickle_file:
                        pickle_data = pickle.load(pickle_file)
                        pickle_data = pickle_data.dropna()
                        pickle_data = pickle_data.reset_index(drop=True)
                        # run if the pickle file is not empty
                        if len(pickle_data) > 0:
                            temp_id = pickle_data['Measuring ID'][0]
                            temp_max = pickle_data['Active Power'].max()
                            # when the 'max_value.csv'file has its submetering data id
                            if temp_id in max_value_df.loc[:, 'id'].to_list():
                                max = int(
                                    max_value_df.loc[max_value_df['id'] == temp_id]['max'])
                                # cmparison of the 'max'value with the 'temp_max(previous max)' value
                                if int(temp_max) > int(max):
                                    max_value_df.loc[(
                                        (max_value_df['id'] == temp_id), 'max')] = temp_max
                                else:
                                    pass
                            # when the 'max_value.csv'file doens not have its submetering data id
                            elif temp_id not in max_value_df.loc[:, 'id'].to_list():
                                max_value_df = max_value_df.append(
                                    {'id': temp_id, 'max': temp_max}, ignore_index=True)
                                pass
                            else:
                                print("Error")
                        else:
                            pass
                else:
                    pass

                pass
        else:
            pass

    # delete rows with max value of '0'
    max_value_df = max_value_df[max_value_df['max'] != 0]
    # save as file 'max_value.csv'
    max_value_df.to_csv('max_value.csv', index=False)
