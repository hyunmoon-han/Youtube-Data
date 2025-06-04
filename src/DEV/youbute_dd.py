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
from concurrent.futures import ThreadPoolExecutor
import yt_dlp


warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUBUTE_DD():
    def __init__(self,seq_number,video_ids):

        #SEQ 번호 초기화
        self.seq_number = seq_number        

        self.input_video_ids = video_ids

        # # DB 연결 정보 load
        with open(os.path.join(src_path, "..","..", "keys","db_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.user = db_keys["user"]
        self.password = db_keys["password"]
        self.host = db_keys["host"]
        self.db = db_keys["db"]
        self.schema = db_keys["schema"]
        self.port = db_keys["port"]


        # 결과 저장 데이터프레임(댓글)
        #self.comment_temp_df = pd.DataFrame(columns = ["comment_id","video_id","channel_id","comment_total_count","comment_text","content_like_count","load_dttm"])

        #self.video_num_temp_df = pd.DataFrame(columns = ["load_dttm","video_id","channel_id","view_count","like_count","comment_count"])

        #self.error_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])
        self.error_df =  pd.DataFrame(columns = ["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

        #self.load_dttm = datetime.now().strftime("%Y%m%d%H")
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
        self.stddt = datetime.now().strftime("%Y%m%d")

        self.fetch_video_id()

    #영상 마스터에서 고정값 정보가 필요한 영상 ID 가져오기
    def fetch_video_id(self):
        conn = self.db_connector()  # SQLAlchemy engine 또는 connection
        query = f"""
            SELECT video_id 
            FROM `{self.schema}`.temp_master
        """
        self.video_df = pd.read_sql(query, con=conn)
    

    #메인 실행함수
    def youtube_crawler_runner(self):
        if self.seq_number==1 :
            video_ids = self.video_df['video_id'].tolist()
        else:
            video_ids = self.input_video_ids
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.master_detail, video_ids))

        # 결과를 DataFrame으로 변환
        df = pd.DataFrame(results)
        # 성공한 작업과 실패한 작업을 구분

        # successful = df[df['status'] == 'success']
        # failed = df[df['status'] == 'failed']
        
        error_df_temp =pd.DataFrame({
                    'stddt': [self.stddt]* len(df['video_id']),
                    'log_seq': self.seq_number,
                    'video_id': df['video_id'],
                    'channel_id': df['channel_id'],
                    'error_flag': df['status'],
                    'program_name': 'youtuby_video_dd',
                    'log_msg' : [None] * len(df['video_id']),
                    'load_dttm': [self.load_dttm]* len(df['video_id']),
                })
        self.error_df = pd.concat([self.error_df, error_df_temp], ignore_index=True)    

        self.db_saver('youtube_error_log',self.error_df)

        successful = df[df['status'] == 'N'].reset_index(drop=True)
        successful['load_dttm']=self.load_dttm

        c_list = ["load_dttm","video_id","channel_id","view_count","like_count","comment_count"]
        successful = successful[c_list]
        self.db_saver('youtube_video_dd',successful)

    
    def master_detail(self,video_id):
        url = f'https://www.youtube.com/watch?v={video_id}'
        ydl_opts = {'quiet': True,'no_warnings': True }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(url, download=False)
                
                # 필요한 정보를 dict로 반환
                return {
                    'video_id': video_id,
                    'channel_id': info.get('channel_id',None),  
                    'view_count': info.get('view_count') or 0,
                    'like_count': info.get('like_count') or 0,  
                    'comment_count': info.get('comment_count') or 0,
                    'status': 'N'  # 성공적인 작업을 구분할 수 있는 필드 추가
                }
            except Exception as e:
                return {
                    'video_id': video_id,
                    'error': str(e),
                    'status': 'Y'  # 실패한 작업을 구분할 수 있는 필드 추가
                }


        


    

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

    topxc = LG_YOUBUTE_DD(1,[])
    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")


