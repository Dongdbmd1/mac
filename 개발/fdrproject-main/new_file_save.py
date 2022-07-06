
import os
from datetime import datetime
import csv
import pysftp
import pandas as pd
from information import sftp_info
from max_value_save import max_value_save_new


def new_file_save():
    '''
    Action to recall the alst updated date and save today's date
    '''
    # today's date
    today_date = datetime.today().strftime("%Y%m%d")
    # read past updated dates
    f = open('download_date.csv', 'r')
    rdr = csv.reader(f)
    date_lst = []
    for line in rdr:
        date_lst.append(line[0])
        pass
    print(date_lst)
    f.close()
    # last of the past dates
    last_date = str(date_lst[-1])
    # year
    last_date_year = last_date[:4]
    # month
    last_date_month = last_date[4:6]
    # save today's date in "download_date.csv" file
    f = open('download_date.csv', 'a', newline='')
    wr = csv.writer(f)
    wr.writerow([today_date])
    f.close()

    '''
    download SFT file 
    '''

    # server login information
    host = sftp_info['host']
    port = sftp_info['port']
    username = sftp_info['username']
    password = sftp_info['password']
    hostkeys = None
    # read all host key information stored on the server
    cnopts = pysftp.CnOpts()
    # verify that host key information exists for the host attempting to connect
    # if the information does not exist, first access is possible by setting 'cnopts.hostkeys' to 'None'
    if cnopts.hostkeys.lookup(host) == None:
        print("Hostkey for" + host + " doesn't exist")
        hostkeys = cnopts.hostkeys  # backing up host key information
        cnopts.hostkeys = None
    # store the host key information for the host on the server if the first connection is successful
    # from the second connection, check the host key and connect
    # enable connection
    with pysftp.Connection(host, port=port, username=username, password=password, cnopts=cnopts) as sftp:
        # code excuted the first time you connections
        if hostkeys != None:
            print("New Host. Caching hostkey for " + host)
            hostkeys.add(host, sftp.remote_server_key.get_name(),
                         sftp.remote_server_key)  # add host and host key
            # hostkeys.save(pysftp.helpers.known_hosts()) # 새로운 호스트 정보 저장
        # save sftp server folder name as industry name information
        indus_info = sftp.listdir('/fxdr')
        # save as csv file (indus_info)
        indus_info_df = pd.DataFrame({'industry': list(indus_info)})
        indus_info_df.to_csv('industry_info.csv',
                             index=False, encoding="cp949")
        path_info = []
        # path to save downloaded data
        csv_path = ""
        # create data folder and save folder name('create_folder_name.csv'에)
        os.makedirs(
            csv_path + "{}_{}".format(date_lst[-1], today_date), exist_ok=True)
        f = open('create_folder_name.csv', 'a', newline='')
        wr = csv.writer(f)
        wr.writerow([csv_path + "{}_{}".format(date_lst[-1], today_date)])
        f.close()
        for indus in indus_info:
            if indus != '#recycle':
                submetering_info = sftp.listdir('/{}/{}'.format('fxdr', indus))
                for submetering in submetering_info:
                    year_info = sftp.listdir(
                        '/{}/{}/{}'.format('fxdr', indus, submetering))
                    for year in year_info:
                        month_info = sftp.listdir(
                            '/{}/{}/{}/{}'.format('fxdr', indus, submetering, year))
                        for month in month_info:
                            # proceed when year and month are equal to or greater than the last save date
                            if int(year) >= int(last_date_year):
                                if int(month) >= int(last_date_month):
                                    sftp.listdir(
                                        '/{}/{}/{}/{}/{}'.format('fxdr', indus, submetering, year, month))
                                    path = '/' + indus + '/' + submetering + '/' + year + '/' + month
                                    path_info.append(path)
                                    remotepath = '/{}/{}/{}/{}/{}'.format(
                                        'fxdr', indus, submetering, year, month)
                                    file_list = sftp.listdir(remotepath)
                                    # folder to store downloaded files(industry, load, year, month)
                                    localpath = csv_path + \
                                        "{}_{}".format(
                                            date_lst[-1], today_date) + "/{}_{}_{}_{}".format(indus, submetering, year, month)
                                    os.makedirs(localpath, exist_ok=True)
                                    df = pd.read_csv(
                                        sftp.open(remotepath + "/" + filename, "r"))
                                    # update max value
                                    max_value_save_new(df)
                                    for filename in file_list:
                                        try:
                                            sftp.get(
                                                remotepath + "/" + filename, localpath + "/" + filename)
                                        except:
                                            try:
                                                # df = pd.read_csv(
                                                #     sftp.open(remotepath + "/" + filename, "r"))
                                                df.to_csv(
                                                    "/" + localpath + "/" + filename, index=False, mode='w', header=True)
                                                df = ''
                                            except:
                                                pass
                                else:
                                    pass
                            else:
                                pass
            else:
                pass

        # ust 'put_d' to upload all the files in the folder at once
        # ex) sftp.put_d('path where files to upload are located', '/')

        # ex) you want to upload multiple files individually, use 'put'serveral times
        # ex) sftp.put('file1 path')
        # ex) sftp.put('file2 path')
        sftp.close()
