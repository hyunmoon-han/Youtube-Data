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




warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))
#크롤링 영상 리스트
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


        # 결과 저장 데이터프레임
        self.result_df = pd.DataFrame(columns = ["video_id","keyword", "chnl_nm", "title", "duration_string", "duration", "url","load_dttm","pick_yn"])

        #에러 테이블 
        self.error_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H")


        self.landing_page_loader()
        time.sleep(1.5)

        #키워드 + DB 적재된 영상 ID가져오기
        self.fetch_video_id()
        

        

    #등록된 키워드들을 검색용 URL로 변환하는 함수
    def keyword_url_maching(self,input_keyword):            

        # 날짜 필터 포함한 검색어 생성-임시로 2025년으로 설정
        query = f"after:2025-01-01 {input_keyword}"

        # URL 인코딩
        encoded_query = urllib.parse.quote_plus(query)

        # 최종 YouTube 검색 URL (4분 이상 필터 포함)
        url = f"https://www.youtube.com/results?search_query={encoded_query}&sp=EgIYAw%3D%3D"

        return url

    #페이지 로딩(초기 화면 띄우는)
    def landing_page_loader(self):
        chrome_options = Options()
        #chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])  # 로그 숨기기
        
        # 페이지 로드
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.get('https://www.youtube.com')
        self.driver.maximize_window()

    #스크롤 아웃 최하단
    def scroll_to_bottom(self,wait_time=2, max_pause=5):

        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        same_height_count = 0

        while True:
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(wait_time)

            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")

            if new_height == last_height:
                same_height_count += 1
                if same_height_count >= max_pause:
                    print("✅ 더 이상 로드할 콘텐츠가 없습니다. 스크롤 종료.")
                    break
            else:
                same_height_count = 0
                last_height = new_height

    #초단위로 바꾸기 함수
    def convert_to_seconds(self,text):
        minutes = 0
        seconds = 0

        if '분' in text:
            parts = text.split('분')
            minutes = int(parts[0].strip())
            text = parts[1]
        if '초' in text:
            seconds = int(text.replace('초', '').strip())

        return minutes * 60 + seconds

    #00:00:00형식 변환하는 함수
    def to_hhmmss(self,text):
        h, m, s = 0, 0, 0
        if '시간' in text:
            parts = text.split('시간')
            h = int(parts[0].strip())
            text = parts[1]
        if '분' in text:
            parts = text.split('분')
            m = int(parts[0].strip())
            text = parts[1]
        if '초' in text:
            s = int(text.replace('초', '').strip())

        return f"{h:02d}:{m:02d}:{s:02d}"

    #조회수를 숫자로 변환하는 함수
    def parse_view_count(self,view_text):
        view_text = view_text.replace("조회수", "").replace("회", "").strip()
        view_text = view_text.replace(",", "")
        if "만" in view_text:
            return int(float(view_text.replace("만", "")) * 10000)
        elif "천" in view_text:
            return int(float(view_text.replace("천", "")) * 1000)
        else:
            return int(re.sub(r'\D', '', view_text)) if re.search(r'\d', view_text) else 0
        
    #실행 함수
    def youtube_crawler_runner(self):

        try:
            for k in range(len(self.input_keyword_df)) :
                input_keyword = self.input_keyword_df.loc[k, 'keyword']
                check_yn = self.input_keyword_df.loc[k, 'del_yn']

                #키워드 테이블에서 신규 등록된 키워드들만 진행행    
                if check_yn =='N':                 
                    #키워드 매칭
                    keyword_url = self.keyword_url_maching(input_keyword)
                    self.driver.get(keyword_url)
                    time.sleep(3)

                    #STEP1: 스크롤 최 하단 이동 
                    self.scroll_to_bottom(wait_time=2, max_pause=3)

                    #STEP2: 활성화된 태그정보 추출 + 전처리리

                    title = []  # 제목
                    duration = []  # 영상 길이 (00:00:00)
                    duration_second = []  # (초)
                    url = []  # 영상 URL
                    video_ids = []  # 영상 video_id

                    # 영상 제목
                    video_titles = self.driver.find_elements(By.ID, 'video-title')
                    time.sleep(1.5)
                    # 영상 길이
                    youtube_title = [title.text.strip() for title in video_titles if title.text.strip()]

                    # 영상 URL
                    youtube_title_url = [title.get_attribute('href') for title in video_titles if title.get_attribute('href')]        
                    time.sleep(1.5)
                    
                    # 영상 길이 (badge-shape) 요소들 찾기
                    # 최대 5초 동안 badge-shape 요소가 로딩될 때까지 대기
                    badges = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'badge-shape.badge-shape-wiz'))
                    )
                    #time.sleep(1.5)
                    youtube_duration = [badge.get_attribute('aria-label').strip() for badge in badges if badge.get_attribute('aria-label') and badge.get_attribute('aria-label').strip()]

                    time.sleep(1.5)
                    print('youtube_title:',len(youtube_title))
                    print('youtube_duration:',len(youtube_duration))
                    print('youtube_title_url:',len(youtube_title_url))
                    
                    # youtube_title, youtube_duration, youtube_title_url 리스트가 길이가 동일한지 확인
                    min_length = min(len(youtube_title), len(youtube_duration), len(youtube_title_url))

                    # 제목과 영상 길이를 매칭하여 출력
                    for i in range(min_length):
                        # 쇼츠 제거
                        if youtube_duration[i] != 'Shorts':
                            title.append(youtube_title[i])
                            duration.append(self.to_hhmmss(youtube_duration[i]))
                            duration_second.append(self.convert_to_seconds(youtube_duration[i]))
                            url.append(youtube_title_url[i])

                            # 해당 URL에서 video_id 추출하여 리스트에 추가
                            video_id = youtube_title_url[i].split("v=")[1].split("&")[0] if 'v=' in youtube_title_url[i] else None
                            video_ids.append(video_id)

                    #추가 : 조회수 
                    # 영상 조회수 정보 추출
                    view_elements = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH, '//span[contains(@class,"inline-metadata-item") and contains(text(), "회")]')
                        )
                    )
                    # 텍스트 추출
                    view_counts = [view.text.strip() for view in view_elements if '회' in view.text]
                    # 형변환
                    view_counts_parsed = [self.parse_view_count(v) for v in view_counts]
                    view_counts_parsed = view_counts_parsed[:min_length]        

                    #채널이름 조회
                    chnl_nms = self.driver.find_elements(By.XPATH, '//ytd-channel-name[@id="channel-name"]//a')
                    time.sleep(1.5)
                    channel_names = [nm.text.strip() for nm in chnl_nms if nm.text.strip() != '']
                    channel_names = channel_names[:min_length] 


                    #STEP3: 데이터프레임 생성
                    df = pd.DataFrame({
                        'video_id' :video_ids,
                        'keyword': [input_keyword] * len(video_ids),  # 키워드 예시 (실제 키워드는 상황에 맞게 할당)            
                        'chnl_nm': channel_names,
                        'title': title,
                        'duration_string': duration,
                        'duration': duration_second,
                        'url': url,
                        'view_cnt':view_counts_parsed,
                        'load_dttm': self.load_dttm,
                        'pick_yn' : 'N'            
                    })  
                    #STEP4: 데이터 필터링
                    df = df[df['view_cnt']>100].reset_index(drop=True)    
                    df = df.drop(columns=['view_cnt'])
                    print('필터링 전 수집된 영상 수:',len(df))
                    #중복되는 값 삭제
                    df = df.drop_duplicates(subset='video_id', keep='first')

                    self.result_df = pd.concat([self.result_df, df], ignore_index=True)

        #에러발생시 에러 로그 추가           
        except Exception as err_msg:
                error_msg = str(err_msg)                
                self.error_df.loc[len(self.error_df)] = {
                    'stddt': self.load_dttm,
                    'video_id': None,
                    'channel_id': None,
                    'error_flag': 'Y',
                    'program_name': 'video_master',
                    'log_msg' : error_msg
                }
                #에러 로그 추가
                self.db_saver('error_log',self.error_df)    
                return

        #프로그램이 정상 작동 했을때 아래 실행행
        # #DB에 저장되어 있는 영상 ID 조회.
        # self.fetch_video_id()

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
            'program_name': 'video_master',
            'log_msg' : error_msg
        }
        if new_cnt > 0:
            self.db_saver('video_master',self.result_df)            
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
            FROM `{self.schema}`.video_master
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
