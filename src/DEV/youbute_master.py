

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

class LG_YOUTUBE_MASTER():
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

        

    #데이터 가져오는 함수 
    def search_youtube(self,video_id,keyword):
       
        url = f'https://www.youtube.com/watch?v={video_id}'
        ydl_opts = {'quiet': True,'no_warnings': True }
        log_msg=None
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(url, download=False)

                #uploader_id = info.get('uploader_id')
                #uploader_id = info.get('channel_id') or info.get('uploader_id')
                #channel_url = f"https://www.youtube.com/{uploader_id}" if uploader_id else '정보 없음'

                #######
                # channel_url = info.get('uploader_url')
                # #초기화
                # uploader_id = None

                # match = re.search(r'youtube\.com/(@[\w\-]+)', channel_url)
                # if match:
                #     uploader_id = match.group(1)
                #######
                #2025.05.28(채널ID부분 변경)
                channel_url = info.get('channel_url',None)
                uploader_id = info.get('channel_id',None)

                #인코딩 추가
                title_raw = info.get('title', '')
                title_normalized = unicodedata.normalize('NFC', title_raw)

                df = pd.DataFrame([{
                    'video_id': video_id,
                    'title': title_normalized,
                    'thumbnail': info.get('thumbnail'),
                    'description': info.get('description'),
                    'channel_id': uploader_id,
                    'channel_nm': info.get('uploader'),
                    'channel_url': channel_url,
                    'categories': info.get('categories', '정보 없음'),
                    'tags': info.get('tags', '정보 없음'),
                    'upload_date': info.get('upload_date', '정보 없음'),
                    'original_url': info.get('original_url'),
                    'duration_string': info.get('duration_string', '정보 없음'),
                    'language': info.get('language', '정보 없음'),
                    'view_cnt' :info.get('view_count'),
                    'is_advertised': info.get('has_ads', False),  # or 'advertised'                    
                    'channel_follower_count': info.get('channel_follower_count') or info.get('uploader_subscriber_count') or info.get('uploader_count'),
                    'log_msg':log_msg,
                    'error_flag':'N',
                    'keyword': keyword,

                }])
                return df
            except Exception as e:                                 
                df = pd.DataFrame([{
                    'video_id': video_id,
                    'title': None,
                    'thumbnail': None,
                    'description': None,
                    'channel_id': None,
                    'channel_nm': None,
                    'channel_url': None,
                    'categories': None,
                    'tags': None,
                    'upload_date': None,
                    'original_url':None,
                    'duration_string': None,
                    'language': None,
                    'view_cnt' :None,
                    'is_advertised': None,  # or 'advertised'                    
                    'channel_follower_count': None,
                    'log_msg':"API 오류",
                    'error_flag':'Y',
                    'keyword': keyword,
                 }])
                return df




    #초단위로 바꾸기 함수
    def convert_to_seconds(self,time_str):
        parts = list(map(int, time_str.strip().split(":")))
        
        if len(parts) == 3:  # hh:mm:ss
            hours, minutes, seconds = parts
        elif len(parts) == 2:  # mm:ss
            hours = 0
            minutes, seconds = parts
        elif len(parts) == 1:  # ss
            hours = 0
            minutes = 0
            seconds = parts[0]
        else:
            return 0  # 잘못된 형식 처리

        return hours * 3600 + minutes * 60 + seconds

    #00:00:00형식 변환하는 함수
    def to_hhmmss(self,time_str):
        parts = time_str.strip().split(":")
        if len(parts) == 2:
            minutes, seconds = parts
            hours = 0
        elif len(parts) == 3:
            hours, minutes, seconds = parts
        else:
            return "00:00:00"  # 예외 처리

        # 0 채워서 hh:mm:ss 형식으로
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        

    # JSON 변환 함수 정의
    def convert_to_json(self,thumbnail):
        data = {
            "url": thumbnail
        }
        return json.dumps(data)
    
    #영상 설명부분 추출
    # def description_script(self,description):        
                
    #     if '#' in description :
    #         #등장하는 앞 부분까지 커트
    #         description= description.split('#')[0]        
        
    #     return description
    def description_script(self,description):

        return re.sub(r'(?:#\S+)+', '', description).strip()
    
    #태그정보 병합
    def merge_tags(self,description, tags):
        # description에서 해시태그 추출
        hashtags = re.findall(r'#(\w+)', description)
        
        if tags is None:
            tags = []
        
        # 중복 제거 후 병합
        merged_tags = list(set(hashtags + tags))
        return merged_tags
    
    #유로광고체크함수
    def is_paid_promotion(self,is_advertised,description):

        #유로광고 배너이미지 확인
        has_paid_content = False
        #설명 부분에 광고 확인
        has_ad = False


        # 광고 키워드
        ad_keywords = [
            "유료 광고", "광고를 포함", "광고 포함", "광고 영상",
            "제작비 지원", "제작비를 지원", "제작 지원", "무상 제공", "제품 제공",
            "협찬받아", "협찬받은", "후원", "스폰서", "스폰서십",
            "제휴", "제휴 링크", "홍보", "리뷰용", "체험단",
            "이 영상은", "광고입니다", "제공받았습니다", "지원받았습니다",
            "PR 샘플", "협업", "브랜드 제공", "브랜드 지원","제작비를 지원받아","수수료를"
        ]

        # 부정 키워드
        negation_keywords = [
            "내돈내산","포함하지 않습니다", "포함하고 있지 않습니다", "광고가 아닙니다","광고 아닙니다", "유료광고가 아닙니다",
            "협찬이 아님","협찬 아닙", "무상 제공 아님", "광고 아님", "스폰서 아님",
            "광고를 포함하지", "광고 없음", "스폰서십이 아닙니다", "제공받지 않음","받지 않습니다"
        ]
        

        # 1. 광고 배너가 있다면 광고 포함
        if is_advertised:
            has_paid_content = True

        # 2.영상 설명 부분에 광고 문구 확인.
        if not any(neg in description for neg in negation_keywords):
            if any(ad in description for ad in ad_keywords):
                has_ad = True



        #하나라도 포함된다면 , 광고 
        has_any_ad = has_paid_content or has_ad
        if has_any_ad:
            
            has_any_ad='Y'
        else:
            
            has_any_ad='N'

        
        return has_any_ad              
    
    #실행 함수
    def youtube_crawler_runner(self):
        #컬럼 리스트 초기화

        #         video_ids = self.video_id_df['video_id'].tolist()
        #         pick_yns = self.video_id_df['pick_yn'].tolist()

        # with ThreadPoolExecutor(max_workers=5) as executor:            
        #     results = list(executor.map(self.search_youtube, video_ids, pick_yns))

        
        if self.seq_number == 1:
            video_keyword_pairs = [
                (vid, kwd) for vid, pyn, kwd in zip(self.video_id_df['video_id'], self.video_id_df['pick_yn'], self.video_id_df['keyword'])
                if pyn == 'N'
            ]
        else:
            video_keyword_pairs = self.input_video_ids

        #신규 영상이 없다면 분기처리
        if len(video_keyword_pairs)==0:
            print('처리할 작업이 없습니다.시스템을 종료합니다')
            return
        print('DB에서 추출해서 작업해야할 수 :',len(video_keyword_pairs))
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda args: self.search_youtube(*args), video_keyword_pairs))


            df = pd.concat(results, ignore_index=True)

            #영상이 shorts가 아닌거 필터 -> 크롤링할때 1차 필터했지만 혹시나 해서 2차필터.
            
            print('영상정보 가져오기 만들어진 데이터 프레임 수:',len(df))
            df = df[~df['original_url'].str.contains('shorts')]
            
            #크롤링 영상 시점 조회수가 100회 미만인거 제외.
            df = df[df['view_cnt']>100].reset_index(drop=True)  
            df = df.drop(columns=['view_cnt'])

            df = df.drop_duplicates(subset='video_id', keep='first')
            print('전처리 후 영상수:',len(df))

            
            # 키워드 ,적재일자,영상길이(초) 컬럼 추가            
            df['stddt'] = self.stddt
            df['load_dttm'] = self.load_dttm

            #데이터 전처리
            # thumbnail JSON 변환
            df['thumbnail'] = df['thumbnail'].apply(self.convert_to_json)
            
            
            #태그정보 병합
            df['tags'] = df.apply(lambda row: self.merge_tags(row['description'], row['tags']), axis=1)
            df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

            #영상 설명 전처리
            df['description'] = df['description'].apply(self.description_script)


            #카테고리 리스트 형 변환
            df['categories'] = df['categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)


            #영상길이 형변환
            df['duration_string'] = df['duration_string'].apply(self.to_hhmmss)
            df['duration'] = df['duration_string'].apply(self.convert_to_seconds)

            #유로광고 적용함수
            df['add_yn'] = df.apply(
                lambda row: self.is_paid_promotion(row['is_advertised'], row['description']),
                axis=1
            )

            #성공
            sucess_df = df[df['error_flag']=='N']
            
            ok_list = [
                "video_id", "keyword", "channel_id", "channel_nm", "channel_url",
                "title", "description", "thumbnail", "tags", "categories",
                "duration_string", "duration", "original_url", "language",
                "upload_date", "load_dttm","add_yn","channel_follower_count"
            ]      
            df['program_name']='temp_master'


            sucess_df = sucess_df[ok_list]
            #최종 성공 
            self.result_df = pd.concat([self.result_df, sucess_df], ignore_index=True)
            
            fail_list =["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"]
            #추가: 시퀀스
            df['log_seq'] =self.seq_number

            fail_df = df[fail_list]
            self.error_df = pd.concat([self.error_df, fail_df], ignore_index=True)
            
            if len(sucess_df) > 0:
                self.db_saver('temp_master',self.result_df)
                print('최종 DB에 적재한 수:',len(self.result_df))            
            else:
                print('추가될 영상 없습니다.')
            
            #에러테이블
            #self.db_saver('error_log',self.error_df)
            self.db_saver('youtube_error_log',self.error_df)
            print('에러 테이블 수 확인 :',len(self.error_df))
            
            #DB에 해당값 업데이트
            self.master_update()

            

  
       #마스터 테이블 업데이트
    def master_update(self):
         # 업데이트할 video_id 목록
        success_ids = self.result_df['video_id'].tolist()

        #업데이트 값 없으면 제외
        if not success_ids:
            return  # 업데이트할 항목 없음

        update_query = text(f"""
            UPDATE `{self.schema}`.temp_id
            SET pick_yn = 'Y'
            WHERE video_id IN :video_ids
        """).bindparams(bindparam("video_ids", expanding=True))

        # SQLAlchemy 2.0 방식: Session 사용
        with Session(self.engine) as session:
            session.execute(update_query, {"video_ids": success_ids})
            session.commit()
   
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
            SELECT DISTINCT video_id,keyword,pick_yn 
            FROM `{self.schema}`.temp_id
        """
        self.video_id_df = pd.read_sql(query, con=conn)

        
    #나중에 추가
    def log_writter(self, msg):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_content = "[%s topx/naver_api_topx.py] %s  "%(current_time, msg)
        print(log_content)  

if __name__ == "__main__":
    topxc = LG_YOUTUBE_MASTER(1,[])

    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")
