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


#라이브러리 사용 영상 리스트 추출
warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUTUBE_CRAWLER():
    def __init__(self):
        # # DB 연결 정보 load
        with open(os.path.join(src_path, "..", "keys","db_keys.json"), "rb") as f:
            db_keys = json.load(f)

        self.user = db_keys["user"]
        self.password = db_keys["password"]
        self.host = db_keys["host"]
        self.db = db_keys["db"]
        self.schema = db_keys["schema"]
        self.port = db_keys["port"]
        
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )


        # 결과 저장 데이터프레임
        self.result_df = pd.DataFrame(columns = ["video_id","keyword","channel_id", "channel_nm","channel_url", "title","description","thumbnail","tags","categories", "duration_string", "duration", "original_url","language","upload_date","load_dttm","add_yn","channel_follower_count"])

        #에러 테이블 
        self.error_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H")
                

        #키워드 + DB 적재된 영상 ID가져오기
        self.fetch_video_id()
        

        

    #데이터 가져오는 함수 
    def search_youtube(self,keyword, max_results=200):
        # 날짜 필터 포함 (2025-01-01 이후)
        query = f"ytsearch{max_results}:after:2025-01-01 {keyword}"
        ydl_opts = {'quiet': True,'no_warnings': True }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            search_result = ydl.extract_info(query, download=False)

        #print('🔎 검색된 영상 수:', len(search_result['entries']))
        
        # 영상 정보를 담을 리스트
        video_data = []
        
        for entry in search_result['entries']:
            # uploader_id를 기반으로 채널 URL 만들기
            uploader_id = entry.get('uploader_id')
            channel_url = f"https://www.youtube.com/{uploader_id}" if uploader_id else '정보 없음'
            
            # 필요한 필드만 추출
            video_info = {
                'video_id': entry.get('id'),
                'title': entry.get('title'),
                'thumbnail': entry.get('thumbnail'),
                'description': entry.get('description'),
                'channel_id': uploader_id,
                'channel_nm': entry.get('uploader'),
                'channel_url': channel_url,
                'categories': entry.get('categories', '정보 없음'),
                'tags': entry.get('tags', '정보 없음'),
                'upload_date': entry.get('upload_date', '정보 없음'),
                'original_url': entry.get('original_url'),
                'duration_string': entry.get('duration_string', '정보 없음'),
                'language': entry.get('language', '정보 없음'),
                'view_cnt' :entry.get('view_count'),
                'is_advertised': entry.get('has_ads', False),  # or 'advertised'
                #'channel_follower_count': entry.get('uploader_count'),
                'channel_follower_count': entry.get('channel_follower_count') or entry.get('uploader_subscriber_count') or entry.get('uploader_count'),
            }
            
            # 리스트에 추가
            video_data.append(video_info)


        
        # DataFrame 생성
        temp_df = pd.DataFrame(video_data)

        #영상이 shorts가 아닌거 필터
        temp_df = temp_df[~temp_df['original_url'].str.contains('shorts')]

        return temp_df
 



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
    def description_script(self,description):        
                
        if '#' in description :
            #등장하는 앞 부분까지 커트
            description= description.split('#')[0]        
        
        return description
    
    #태그정보 병합합
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
        desired_columns = [
                        "video_id", "keyword", "channel_id", "channel_nm", "channel_url",
                        "title", "description", "thumbnail", "tags", "categories",
                        "duration_string", "duration", "original_url", "language",
                        "upload_date", "load_dttm","add_yn","channel_follower_count"
                    ]
        try:
            for k in tqdm(range(len(self.input_keyword_df))) :
                input_keyword = self.input_keyword_df.loc[k, 'keyword']
                check_yn = self.input_keyword_df.loc[k, 'del_yn']

                #키워드 테이블에서 삭제처리 안된 키워드 진행 
                if check_yn =='N':                 
                    
                    #DB에 적재된 키워드로 등록된 영상들 조회
                    temp_df = self.search_youtube(input_keyword,max_results=200)
                                                                                                                                                                                                     
                    # 조회수 필터링(100회이상상)
                    print('수집 영상 수:',len(temp_df))
                    temp_df = temp_df[temp_df['view_cnt']>100].reset_index(drop=True)    
                    temp_df = temp_df.drop(columns=['view_cnt'])

                    # 중복되는 값 삭제
                    temp_df = temp_df.drop_duplicates(subset='video_id', keep='first')
                    print('전처리 후 영상수:',len(temp_df))

                    # 키워드 ,적재일자,영상길이(초) 컬럼 추가
                    temp_df['keyword'] = input_keyword
                    temp_df['load_dttm'] = self.load_dttm
                    
                    
                    #데이터 전처리
                    # thumbnail JSON 변환
                    temp_df['thumbnail'] = temp_df['thumbnail'].apply(self.convert_to_json)

                    #영상 설명 전처리
                    temp_df['description'] = temp_df['description'].apply(self.description_script)

                    #태그정보 병합
                    temp_df['tags'] = temp_df.apply(lambda row: self.merge_tags(row['description'], row['tags']), axis=1)
                    temp_df['tags'] = temp_df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

                    #카테고리 리스트 형 변환
                    temp_df['categories'] = temp_df['categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)


                    #영상길이 형변환
                    temp_df['duration_string'] = temp_df['duration_string'].apply(self.to_hhmmss)
                    temp_df['duration'] = temp_df['duration_string'].apply(self.convert_to_seconds)

                    #유로광고 적용함수
                    temp_df['add_yn'] = temp_df.apply(
                        lambda row: self.is_paid_promotion(row['is_advertised'], row['description']),
                        axis=1
                    )

                    # 컬럼 순서 재정렬
                    temp_df = temp_df[desired_columns]

                    self.result_df = pd.concat([self.result_df, temp_df], ignore_index=True)
        

        #에러발생시 에러 로그 추가           
        except Exception as err_msg:
                error_msg = str(err_msg)                
                self.error_df.loc[len(self.error_df)] = {
                    'stddt': self.load_dttm,
                    'video_id': None,
                    'channel_id': None,
                    'error_flag': 'Y',
                    'program_name': 'youtube_video_m',
                    'log_msg' : error_msg
                }
                #에러 로그 추가
                self.db_saver('error_log',self.error_df)    
                return

        #프로그램이 정상 작동 했을때 아래 실행행
        # #DB에 저장되어 있는 영상 ID 조회.
        # self.fetch_video_id()
        finally:
            # video_id 기준으로 self.keyword_df에 포함되지 않은 것만 필터링
            self.result_df = self.result_df[~self.result_df['video_id'].isin(self.video_id_df['video_id'])].reset_index(drop=True)
            
            new_cnt = self.result_df.shape[0]
            print(f'신규로 추가된 영상은 "{new_cnt}"개 입니다')

            error_msg = None
            #에러테이블 업데이트
            self.error_df.loc[len(self.error_df)] = {
                'stddt': self.load_dttm,
                'video_id': None,
                'channel_id': None,
                'error_flag': 'N',
                'program_name': 'youtube_video_m',
                'log_msg' : error_msg
            }
            if new_cnt > 0:
                self.db_saver('youtube_video_m_temp',self.result_df)            
            else:
                print('추가될 영상 없습니다.')

            self.db_saver('error_log',self.error_df)            
   
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
            SELECT DISTINCT video_id 
            FROM `{self.schema}`.youtube_video_m
        """
        self.video_id_df = pd.read_sql(query, con=conn)

        # 키워드 조회
        query_keyword = f"""
            SELECT keyword, del_yn 
            FROM `{self.schema}`.input_keyword
        """
        self.input_keyword_df = pd.read_sql(query_keyword, con=conn)
       
        

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
