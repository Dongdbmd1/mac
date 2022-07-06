import pandas as pd
import numpy as np
import os
import pickle
import csv


def data_pickling():
    '''
    organize folder name that contain data to pickle
    '''
    # path containing the file (listed in file 'create_folder_name.csv')
    f = open('create_folder_name.csv', 'r', encoding='utf-8-sig')
    rdr = csv.reader(f)
    date_lst = []
    for line in rdr:
        date_lst.append(line[0])
        pass
    print(date_lst)
    f.close()

    # folder deduplication(don't use 'set' function to maintain order)
    newlist = []
    for x in date_lst:
        if x not in newlist:
            newlist.append(x)
    # order reverse
    newlist.reverse()

    sub_folder_lst = []
    # save a list of folders within a date(ex. 20220324_20220414) folder
    # sub_folder_lst(ex: industry_submetering id_year_month)
    for folder in newlist:
        folder_lst = os.listdir("..\\{}".format(folder))
        sub_folder_lst += folder_lst

    sub_folder_lst = list(set(sub_folder_lst))

    processed_data_lst = []
    '''
    pickling data within a folder by going around the folder list
    '''
    # sub_folder(i = ex. 세방전기_C413901320012620000705_2022_05)
    for i in sub_folder_lst:
        if i != "desktop.ini":
            # folder(ex. 20220324_20220414)
            for folder in newlist:
                if i not in processed_data_lst:
                    folder_lst = os.listdir("..\\{}".format(folder))
                    if i in folder_lst:
                        try:
                            csv_path = "..\\{}\\{}".format(folder, i)
                            folder_lst = os.listdir(csv_path)
                            file_list_py = [
                                file for file in folder_lst if file.endswith('.csv')]

                            save_file = pd.DataFrame()

                            # run only when there are files in that folder
                            if len(file_list_py) != 0:
                                for j in file_list_py:
                                    data = pd.read_csv(csv_path + '\\' + j)
                                    save_file = pd.concat([save_file, data])

                                save_file = save_file[[
                                    'Measuring Time', 'Measuring ID', 'Active Power']]
                                save_file = save_file.reset_index(drop=True)

                                # divide str values by "_"
                                split_csv_file_name = i.split('_')
                                industry_name = split_csv_file_name[0]
                                submetering_name = split_csv_file_name[1]
                                year_name = split_csv_file_name[2]
                                month_name = split_csv_file_name[3]
                                try:
                                    # create 'industry'folder in 'pickle_folder' folder
                                    os.makedirs(
                                        '..\\pickle_folder' + "\\{}".format(industry_name), exist_ok=True)
                                except:
                                    pass
                                industry_pickle_path = '..\\pickle_folder\\{}'.format(
                                    industry_name)

                                # save "industry_year_month.pickle" in industry folder
                                save_file_name = open(industry_pickle_path + "\\{}_{}_{}.pickle".format(
                                    submetering_name, year_name, month_name), "wb")
                                pickle.dump(save_file, save_file_name)
                                save_file_name.close()
                        except:
                            pass
                    else:
                        pass
                        processed_data_lst.append(i)
                        pass
                else:
                    pass
        else:
            pass
