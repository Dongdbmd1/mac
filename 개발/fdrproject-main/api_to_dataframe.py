import xml.etree.ElementTree as ET
import pandas as pd
from urllib.request import urlopen
from pprint import pprint


class api_to_dataframe():

    
    # https://www.data.go.kr/dataset/15012690/openapi.do

    # year,month str로 들어가야함, 1월 > 01
    @staticmethod
    def get_api_data(year,month):

        # 년단위로 받아오는 매소드 작업중
        # 현재는 월단위
        url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo?'\
        +'solYear=' + year \
        +'&solMonth=' + month \
        +'&ServiceKey=' \
        +'viIbaOcTJNKfa%2Bd8oJAHBuvn7LUooDFjSkSLEENFBqeuj4UqJuxMYQouC%2FcvxeZ39RO3z8juvF70or%2B8SwrayA%3D%3D'
        
        # 요청을 보내서 결과를 받아옴
        response = urlopen(url).read().decode('utf-8')
        
        # xml파일 읽어옴
        response_data = ET.ElementTree(ET.fromstring(response))
        root = response_data.getroot()

        data_list = []

        n_locdate=''

        for child in root:
            if child.tag == 'body':
                for body in child:
                    if body.tag == 'items':
                        for items in body:
                            for item in items:
                                if item.tag == 'locdate':
                                    n_locdate = item.text
                                    data_list.append({'locdate':n_locdate})

        # 결과 dataframe으로 변환
        holiday_df = pd.DataFrame(data_list,columns = ['locdate'])
        return holiday_df


if __name__ == "__main__":
    a = api_to_dataframe()
    print(a.get_api_data('2020','09'))


        