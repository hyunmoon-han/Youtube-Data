

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
import yt_dlp
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import text, bindparam
import unicodedata


warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))
#설명 부분 필터
class LG_YOUTUBE_CRAWLER():
    def __init__(self):
        # # DB 연결 정보 load
        with open(os.path.join(src_path, "..","..", "keys","db_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.user = db_keys["user"]
        self.password = db_keys["password"]
        self.host = db_keys["host"]
        self.db = db_keys["db"]
        self.schema = db_keys["schema"]
        self.port = db_keys["port"]


        # 결과 저장 데이터프레임
        self.result_df = pd.DataFrame(columns = ["video_id","keyword","channel_id", "channel_nm","channel_url", "title","description","thumbnail","tags","categories", "duration_string", "duration", "original_url","language","upload_date","load_dttm","add_yn","channel_follower_count"])

        #에러 테이블 
        #self.error_df =  pd.DataFrame(columns = ["stddt","log_seq","video_id", "channel_id", "error_flag", "program_name"])
        self.error_df =  pd.DataFrame(columns =["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

        #적재 시간
        #self.load_dttm = datetime.now().strftime("%Y%m%d%H")
        #적재일자(년월일)
        self.stddt = datetime.now().strftime("%Y%m%d")
        #(년월일시분분)
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
                

        #키워드 + DB 적재된 영상 ID가져오기
        self.fetch_video_id()
        
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )

        

   

    def check_keyword_match(self,row):
        keyword = str(row['keyword']).lower()
        # 각 필드에 keyword가 포함되어 있는지 확인 (NaN 방지 위해 str 변환)
        in_title = keyword in str(row['title']).lower()
        in_desc = keyword in str(row['description']).lower()
        in_tags = keyword in str(row['tags']).lower()
        return 'Y' if in_title or in_desc or in_tags else 'N'

    
  
    #실행 함수
    def youtube_crawler_runner(self):
        # 새 컬럼 'keyword_matched' 생성
        self.video_id_df['keyword_matched'] = self.video_id_df.apply(self.check_keyword_match, axis=1)
        for _, row in self.video_id_df.iterrows():
            print(f"영상 {row['video_id']}: {row['keyword_matched']}")


  

   
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



    #영상마스터의 기존 영상ID가져오기 + 키워드 테이블 값 가져오기
    def fetch_video_id(self):
        conn = self.db_connector()  # SQLAlchemy engine 또는 connection
        query = f"""
            SELECT DISTINCT video_id,keyword,title,description,tags  
            FROM `{self.schema}`.temp_master
        """
        self.video_id_df = pd.read_sql(query, con=conn)

        
    #나중에 추가
    def log_writter(self, msg):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_content = "[%s topx/naver_api_topx.py] %s  "%(current_time, msg)
        print(log_content)  

if __name__ == "__main__":
    topxc = LG_YOUTUBE_CRAWLER()

    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")
