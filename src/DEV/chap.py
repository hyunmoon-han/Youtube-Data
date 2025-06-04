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
#챕터 부분 (수정중 -)
class LG_YOUBUTE_DETAIL_NUM():
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
        
        #video_ids = self.video_df['video_id'].tolist()
        video_ids = 'SJ2vps3VjAc'
        self.master_detail(video_ids)

    
    def master_detail(self,video_id):
        url = f'https://www.youtube.com/watch?v={video_id}'
        ydl_opts = {'quiet': True,'no_warnings': True }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:         
            info = ydl.extract_info(url, download=False)
            chapters = info.get('chapters')
            if chapters:
                for c in chapters:
                    print(f"{c['start_time']}s ~ {c['end_time']}s : {c['title']}")


        


    

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

    topxc = LG_YOUBUTE_DETAIL_NUM()
    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")


