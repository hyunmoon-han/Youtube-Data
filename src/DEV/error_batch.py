import yt_dlp
import os
from pathlib import Path
import re
import pandas as pd
import shutil
import time
import pandas as pd
import numpy as np
import re
import json
from datetime import datetime, timedelta
import awswrangler as wr
from selenium.webdriver.support import expected_conditions as EC
import os
import warnings
import re
from sqlalchemy import create_engine
import yt_dlp
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy import text, bindparam

#다른 파일 추가
from youbute_master_temp import LG_YOUTUBE_MASTER_TEMP
from youbute_master import LG_YOUTUBE_MASTER
from youbute_script import LG_YOUTUBE_SCRIPT
from youbute_dd import LG_YOUBUTE_DD
from youbute_comment import LG_YOUBUTE_COMMENT

src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUBUTE_DETAIL_COMMENT():
    def __init__(self):

        # # DB 연결 정보 load
            # # DB 연결 정보 load
        with open(os.path.join(src_path,'..','..', "keys","db_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.user = db_keys["user"]
        self.password = db_keys["password"]
        self.host = db_keys["host"]
        self.db = db_keys["db"]
        self.schema = db_keys["schema"]
        self.port = db_keys["port"]

        with open(os.path.join(src_path, '..','..', "keys","api_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.api_key = db_keys["api_key"]


        # 결과 저장 데이터프레임(댓글)
        self.comment_df = pd.DataFrame(columns = ["comment_id","video_id","channel_id","comment_text","content_like_count","load_dttm"])

        self.comment_temp_df = pd.DataFrame(columns = ["comment_id","video_id","comment_text","content_like_count",'channel_id'])

        
        self.error_df =  pd.DataFrame(columns =["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
        self.stddt = datetime.now().strftime("%Y%m%d")
        
        

        
#temp_id
#temp_master
#youtube_video_script
#youtuby_video_dd
#youtube_video_comment_dd
#프로그램 순서
            



    def db_connector(self):
        connection_url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        conn = create_engine(connection_url)
        return conn
    
    #당일날짜에 대해서 에러난 부분 추출
    def error_flag_check(self,program_name):
        conn = self.db_connector()  # SQLAlchemy engine 또는 connection
        query = f"""
                SELECT log_seq
                    , video_id  
                    , channel_id                  
                FROM `{self.schema}`.youtube_error_log
                WHERE 1=1
                AND program_name='{program_name}'
                AND stddt ='{self.stddt}'
                AND error_flag ='Y'
                AND load_dttm = (SELECT MAX(load_dttm)
                                    FROM `{self.schema}`.youtube_error_log
                                    WHERE 1=1
                                    AND program_name='{program_name}'
                                    AND stddt ='{self.stddt}'
                                    AND error_flag ='Y' 
                                    )
        """
        self.video_df = pd.read_sql(query, con=conn)
    
    #메인 실행 함수 
    def moon(self):
        #temp_id
        print('temp_id 작업 시작')
        self.error_flag_check('temp_id')
        if self.video_df.empty:
            print('데이터 없음.프로그램 종료')
            print('temp_id 작업 끝')
            #return
        else:
            #작업해야할 목록 리스트업  -> 크롤링 부분(키워드별로 에러 데이터 적재)
            seq_number = int(self.video_df['log_seq'].iloc[0]) + 1                                            
            youbute_temp = LG_YOUTUBE_MASTER_TEMP(seq_number)     
            youbute_temp.youtube_crawler_runner()           
            print('temp_id 작업 끝')

        #temp_master
        print('temp_master 작업 시작')
        self.error_flag_check('temp_master')
        if self.video_df.empty:            
            print('데이터 없음.프로그램 종료')
            print('temp_master 작업 끝')
        else:        
            for i in range(len(self.video_df)):
                print(self.video_df['log_seq'].iloc[i], self.video_df['video_id'].iloc[i])
            video_ids = self.video_df['video_id'].tolist()
            seq_number = int(self.video_df['log_seq'].iloc[0]) + 1                                            
            youbute_master =LG_YOUTUBE_MASTER(seq_number,video_ids)
            youbute_master.youtube_crawler_runner()
            print('temp_master 작업 끝')

        #youtube_video_script
        print('youtube_video_script 작업 시작')
        self.error_flag_check('youtube_video_script')
        if self.video_df.empty:            
            print('데이터 없음.프로그램 종료')            
            print('youtube_video_script 작업 끝')
        else:
            for i in range(len(self.video_df)):
                print(self.video_df['log_seq'].iloc[i], self.video_df['video_id'].iloc[i])
            video_ids = self.video_df['video_id'].tolist()
            seq_number = int(self.video_df['log_seq'].iloc[0]) + 1                                            
            youbute_script = LG_YOUTUBE_SCRIPT(seq_number,video_ids)
            youbute_script.youtube_crawler_runner()
            print('youtube_video_script 작업 끝')

        #youtuby_video_dd
        print('youtuby_video_dd 작업 시작')
        self.error_flag_check('youtuby_video_dd')
        if self.video_df.empty:            
            print('데이터 없음.프로그램 종료')
            print('youtuby_video_dd 작업 끝')
            #return
        else:        
            for i in range(len(self.video_df)):
                print(self.video_df['log_seq'].iloc[i], self.video_df['video_id'].iloc[i])
            video_ids = self.video_df['video_id'].tolist()
            seq_number = int(self.video_df['log_seq'].iloc[0]) + 1                                            
            youbute_dd = LG_YOUBUTE_DD(seq_number,video_ids)
            youbute_dd.youtube_crawler_runner()
            print('youtuby_video_dd 작업 끝')





if __name__ == "__main__":
    topxc = LG_YOUBUTE_DETAIL_COMMENT()
    start_time = time.time()
    topxc.moon()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")
