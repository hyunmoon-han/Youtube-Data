
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
import time
import pandas as pd
import numpy as np
import re
import json
from datetime import datetime, timedelta
import awswrangler as wr
from awswrangler.exceptions import EmptyDataFrame
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, parse_qs
import os
import warnings
import re
import urllib.parse
import mysql.connector
from sqlalchemy import create_engine
import uuid
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor




warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUBUTE_COMMENT():
    def __init__(self,seq_number):

        #SEQ 번호 초기화
        self.seq_number = seq_number  

        # # DB 연결 정보 load
            # # DB 연결 정보 load
        with open(os.path.join(src_path, "..",".." ,"keys","db_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.user = db_keys["user"]
        self.password = db_keys["password"]
        self.host = db_keys["host"]
        self.db = db_keys["db"]
        self.schema = db_keys["schema"]
        self.port = db_keys["port"]

        with open(os.path.join(src_path, "..","..", "keys","api_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.api_key = db_keys["api_key"]


        # 결과 저장 데이터프레임(댓글)
        self.comment_df = pd.DataFrame(columns = ["comment_id","video_id","channel_id","comment_text","content_like_count","load_dttm"])

        self.comment_temp_df = pd.DataFrame(columns = ["comment_id","video_id","comment_text","content_like_count",'channel_id'])

        #self.error_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name","log_msg"])
        self.error_df =  pd.DataFrame(columns =["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
        self.stddt = datetime.now().strftime("%Y%m%d")
        
        

        self.fetch_video_id()
            
            
        # API URL
        self.url = 'https://www.googleapis.com/youtube/v3/commentThreads'

    #비디오 영상 하나당 처리 함수
    def comment_master_detail(self, video_id,channel_id):
        all_comments = []
        next_page_token = None

        try:
            while True:
                data = self.fetch_comment_page(video_id, self.api_key, page_token=next_page_token)

                if not data:
                    break

                comments = self.parse_comments(data, video_id)
                all_comments.extend(comments)

                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    break

            #댓글정보 없을때때
            if not all_comments:
                # 댓글이 없는 경우 → 정상 처리지만 결과 없음
                return pd.DataFrame([{
                    'video_id': video_id,
                    'channel_id': channel_id,
                    'comment_id': None,
                    'comment_text': None,
                    'content_like_count': None,
                    'status': 'N',
                    'log_msg': '댓글 없음'
                }])

            # 성공 결과 → DataFrame 생성
            df = pd.DataFrame(all_comments)
            df['channel_id'] = channel_id
            df['status'] = 'N'
            df['log_msg'] = None



            return df
        
        except Exception as e:
            # 실패 결과 → 최소 정보 포함
            return pd.DataFrame([{
                'video_id': video_id,
                'channel_id': channel_id,
                'comment_id': None,
                'comment_text': None,
                'content_like_count': None,
                'status': 'Y',
                'log_msg': str(e)
            }])
    
    #메인 실행 함수
    def youtube_crawler_runner(self):
        video_ids = self.video_df['video_id'].tolist()
        channel_ids = self.video_df['channel_id'].tolist()
        print('작업해야할 영상 수 확인:',len(video_ids))

        with ThreadPoolExecutor(max_workers=5) as executor:
            #results = list(executor.map(self.comment_master_detail, video_ids))
            results = list(executor.map(self.comment_master_detail, video_ids,channel_ids))

        # 전체 결과 결합
        df = pd.concat(results, ignore_index=True)
        
        df['load_dttm'] = self.load_dttm

        # 컬럼 정리-원본
        cols = ["comment_id", "video_id", "channel_id", "comment_text", "content_like_count", "load_dttm", "status",'log_msg']
        df = df[cols]


        cols2 = ["comment_id", "video_id", "channel_id", "comment_text", "content_like_count", "load_dttm"]    
        temp_df = df[df['status'] == 'N'].reset_index(drop=True)
        s_unique_count = temp_df['video_id'].nunique()
        print("성공한 영상 개수:", s_unique_count)

        
        temp_df2 = df[df['status'] == 'Y'].reset_index(drop=True)
        f_unique_count = temp_df2['video_id'].nunique()
        print("실패 영상 개수:", f_unique_count)
        
        temp_df = temp_df[cols2]

        # 최종 저장-정상 작동한 것들만 DB에 적재      
        self.comment_df = pd.concat([self.comment_df, temp_df], ignore_index=True)           
        self.db_saver('youtube_video_comment_dd', self.comment_df)


        
        
        # 영상별로 그룹화하여 첫 번째 에러 메시지만 남기기
        error_df = df.groupby('video_id').agg(
            channel_id=('channel_id', 'first'),
            error_flag=('status', lambda x: 'N' if 'N' in x.values else 'Y'),
            log_msg=('log_msg', 'first')  # 추가된 부분
        ).reset_index()
        # 고정값 컬럼 추가
        error_df['stddt'] = self.stddt
        error_df['program_name']='youtube_video_comment_dd'
        error_df['log_seq']=self.seq_number
        error_df['load_dttm']=self.load_dttm
        #error_df['log_msg'] = None

        
        #error_list = ["stddt","video_id", "channel_id", "error_flag", "program_name","log_msg"]
        error_list = ["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"]
        error_df= error_df[error_list]

        self.db_saver('youtube_error_log', error_df)        



    #  요청 함수 
    def fetch_comment_page(self,video_id, api_key, page_token=None):
        
        params = {
            'part': 'snippet',
            'videoId': video_id,
            'key': api_key,
            'maxResults': 100,
            'textFormat': 'plainText'
        }
        if page_token:
            params['pageToken'] = page_token

        response = requests.get(self.url, params=params)

        if response.status_code != 200:
            print(f"API 요청 실패: {response.status_code} - {response.text}")
            #raise Exception(f"API 요청 실패: {response.status_code} - {response.text}")
            return None  # 실패 시 None 반환
        
        return response.json()        
        
        
    #요청 값 데이터 전처리리
    def parse_comments(self,json_data, video_id):
        comments = []
        for item in json_data.get('items', []):
            snippet = item['snippet']['topLevelComment']['snippet']
            comment_id = item['snippet']['topLevelComment']['id']
            
            comments.append({
                'comment_id': comment_id,
                'video_id': video_id,                
                'comment_text': snippet['textDisplay'],
                'content_like_count': snippet['likeCount']
            })
        return comments        


    #영상 마스터에서 고정값 정보가 필요한 영상 ID 가져오기
    def fetch_video_id(self):
        conn = self.db_connector()  # SQLAlchemy engine 또는 connection
        query = f"""
            SELECT video_id,channel_id 
            FROM `{self.schema}`.temp_master
        """
        self.video_df = pd.read_sql(query, con=conn)







    def db_connector(self):
        connection_url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        conn = create_engine(connection_url)
        return conn

    def db_saver(self, target_db_table, dataframe):
        # DB 연결 객체
        conn = self.db_connector()
        # DB 데이터 적재
        dataframe.to_sql(
            name=target_db_table,
            con=conn,
            schema=self.schema,
            if_exists="append",
            index=False
        )
        conn.dispose()
    
    def log_writter(self, msg):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_content = "[%s topx/naver_api_topx.py] %s  "%(current_time, msg)
        print(log_content)  

if __name__ == "__main__":

    topxc = LG_YOUBUTE_COMMENT(1)
    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")