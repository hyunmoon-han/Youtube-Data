
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


#영상 일 데이터 잭재(크롤링)
warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUBUTE_DETAIL_NUM():
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


        # 결과 저장 데이터프레임(댓글)
        self.comment_temp_df = pd.DataFrame(columns = ["comment_id","video_id","channel_id","comment_total_count","comment_text","content_like_count","load_dttm"])

        self.video_num_temp_df = pd.DataFrame(columns = ["video_id","channel_id","channel_follower_count","view_count","like_count","load_dttm"])

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")


        self.landing_page_loader()
        time.sleep(1.5)
        



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
        self.driver.get('https://www.youtube.com/watch?v=1uttWCALisM&pp=ygUiYWZ0ZXI6MjAyNS0wMS0wMSBMR-uhnOu0h-yyreyGjOq4sA%3D%3D')
        self.driver.maximize_window()
        time.sleep(3)

    
    
    #영상 좋아요 크롤링
    def like_count(self):
        like_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="top-level-buttons-computed"]/segmented-like-dislike-button-view-model/yt-smartimation/div/div/like-button-view-model/toggle-button-view-model/button-view-model/button'))
        )

        # 좋아요 수 추출 (aria-label 또는 textContent 등 시도)
        like_text = like_button.get_attribute("aria-label") or like_button.text.strip()

        # 숫자만 추출 (콤마 제거)
        like_text = re.search(r'([\d,]+)', like_text)
        if like_text:
            like_count = like_text.group(1).replace(',', '')
        print('영상 좋아요 통과')
        return like_count
    
    #영상ID 추출하는 함수
    def thumbnail_video_id(self):
        # 현재 유튜브 영상 URL
        video_url = self.driver.current_url

        # 영상 ID 추출
        parsed = urlparse(video_url)
        video_id = parse_qs(parsed.query).get('v', [None])[0]


        #영상ID 
        print('영상ID통과')
        return video_id
    


    #메인 실행함수
    def youtube_crawler_runner(self):
        #더보기 클릭
        self.driver.find_element(By.XPATH,'//*[@id="expand"]').click()        
        time.sleep(1.5)  

        #영상 제목
        title = self.driver.find_element(By.XPATH, '//*[@id="title"]/h1/yt-formatted-string').text
        time.sleep(1.5)  
        
        #썸네일정보 + 영상ID 
        video_id = self.thumbnail_video_id()



        #채널ID,이름 추출
        channel_id  = self.chnnl_script()

        #채널구독자 카운트 추출
        channel_follower_count = self.channel_follower_count_script()
        channel_follower_count =int(channel_follower_count)

        #영상 좋아요 카운트 추출
        like_count = self.like_count()
        like_count = int(like_count)

        #영상 조회수 + 업로드 일자 
        view_count = self.view_count_upload_script()
        view_count = int(view_count)

        #스크롤 최하단 이동
        self.scroll_to_load_comments(wait_time=2, max_pause=3)

        #댓글 총 개수 추출
        comment_count = self.driver.find_element(By.XPATH,'//*[@id="leading-section"]//h2[@id="count"]//span[2]').text
        comment_total_count = int(comment_count)
        
        #댓글정보 추출
        comment_id,comment_text,content_like_count=self.comment_script()


        #원본   
        # df = pd.DataFrame({
        #     'comment_id':comment_id,
        #     'video_id': [video_id] * len(comment_id), 
        #     'channel_id': [channel_id] * len(comment_id),                            
        #     'channel_follower_count': [channel_follower_count] * len(comment_id),                                 
        #     'view_count': [view_count] * len(comment_id),                                         
        #     'like_count': [like_count] * len(comment_id),                                         
        #     'comment_total_count':[comment_total_count] * len(comment_id),            
        #     'comment_text':comment_text,  
        #     'content_like_count':content_like_count,      
        #     'load_dttm':[self.load_dttm] * len(comment_id)
        # })
        


        #데이터프레임 생성(댓글)
        comment_df = pd.DataFrame({
            'comment_id':comment_id,
            'video_id': [video_id] * len(comment_id), 
            'channel_id': [channel_id] * len(comment_id),                                                                                                                                        
            'comment_total_count':[comment_total_count] * len(comment_id),            
            'comment_text':comment_text,  
            'content_like_count':content_like_count,      
            'load_dttm':[self.load_dttm] * len(comment_id)
        })
        self.comment_temp_df = pd.concat([self.comment_temp_df, comment_df], ignore_index=True)
        self.db_saver('video_comment_daily',self.comment_temp_df)


        #데이터프레임 생성(수치형형)
        video_num_df = pd.DataFrame({            
            'video_id': [video_id] , 
            'channel_id': [channel_id] ,                            
            'channel_follower_count': [channel_follower_count] ,                                 
            'view_count': [view_count] ,                                         
            'like_count': [like_count] ,                                                 
            'load_dttm':[self.load_dttm] 
        })
        
        self.video_num_temp_df = pd.concat([self.video_num_temp_df, video_num_df], ignore_index=True)
        
        self.db_saver('video_num_daily',self.video_num_temp_df)
        


        
    
    #채널정보 추출
    def chnnl_script(self):
        #채널 URL가져오기.
        channel_url = self.driver.find_element(By.XPATH,'//*[@id="owner"]/ytd-video-owner-renderer/a').get_attribute("href")                                    
        time.sleep(1.5)
               
        #채널 ID
        match = re.search(r'/(@[\w\d_]+)', channel_url)
        if match:
            channel_id = match.group(1)
             
        time.sleep(1.5)
        print('채널정보 통과')    
        return channel_id   
    
    #구독자 수 추출
    def channel_follower_count_script(self):
        #구독자 구하기.
        text = self.driver.find_element(By.XPATH, '//*[@id="owner-sub-count"]').text
        time.sleep(1.5)
        # '만'을 10000으로 바꾸고 숫자 부분만 처리
        if '만' in text:
            # '구독자'와 '만명'을 제외하고 숫자만 추출
            number_str = text.replace('만명', '').replace('구독자', '').strip()

            # 소수점을 정수로 변환 (소수점이 있을 경우)
            number = float(number_str) * 10000  # '만'을 10000으로 변환
            channel_follower_count = int(number)  # 정수로 변환
        else:
            # '만'이 없다면 그냥 숫자로 출력
            channel_follower_count = int(text.replace('구독자', '').replace('명', '').strip())

        print('구독자 수 통과')
        return channel_follower_count

       

    #조회수 
    def view_count_upload_script(self) :
        #조회수 초기화
        view_count = None

        spans = self.driver.find_elements(By.CSS_SELECTOR, 'span.bold[style-target="bold"]')
        time.sleep(1.5)
        for span in spans:
            text = span.text.strip()
            # 업로드 일자 (날짜) 추출
            if '조회수' in text :  # 텍스트가 비어있지 않은 경우
                match = re.search(r'(\d{1,3}(?:,\d{3})*)', text)
                if match:
                    view_count = match.group(1).replace(',', '')  # 콤마 제거하고 숫자만 가져오기
                                  
        print('조회수+업로드일자 통과')
        return view_count      


    #스크롤 아웃 최하단
    # def scroll_to_bottom(self,wait_time=2, max_pause=5):

    #     last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
    #     same_height_count = 0

    #     while True:
    #         self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
    #         time.sleep(wait_time)

    #         new_height = self.driver.execute_script("return document.documentElement.scrollHeight")

    #         if new_height == last_height:
    #             same_height_count += 1
    #             if same_height_count >= max_pause:
    #                 print("더 이상 로드할 콘텐츠가 없습니다. 스크롤 종료.")
    #                 break
    #         else:
    #             same_height_count = 0
    #             last_height = new_height

    def scroll_to_load_comments(self, wait_time=2, max_pause=3):

        last_comment_count = 0
        same_count_repeat = 0

        while True:
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(wait_time)

            # 현재 댓글 수 확인
            comments = self.driver.find_elements(By.CSS_SELECTOR, 'ytd-comment-thread-renderer')
            current_comment_count = len(comments)

            if current_comment_count == last_comment_count:
                same_count_repeat += 1
                if same_count_repeat >= max_pause:
                    print("더 이상 댓글이 로드되지 않습니다. 스크롤 종료.")
                    break
            else:
                same_count_repeat = 0
                last_comment_count = current_comment_count

    #댓글 정보 추출출
    def comment_script(self):
        # 댓글이 로드될 때까지 최대 3초 대기
        comments = WebDriverWait(self.driver, 3).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ytd-comment-thread-renderer'))
        )
        #초기화
        comment_ids = []
        content_texts= []
        content_like_counts = [] 
        
        for comment in comments:
            # 댓글 고유 ID
            comment_id = uuid.uuid4().hex
            comment_ids.append(comment_id)
            
            #초기화
            content_text = None
            content_like_count = None

            # 댓글 내용 (최대 3초 대기)
            content_text = WebDriverWait(comment, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#content-text'))
            ).text.strip()
            content_texts.append(content_text)

            # 좋아요 수 (최대 3초 대기)
            content_like_count = WebDriverWait(comment, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#vote-count-middle'))
            ).text.strip()
            if content_like_count =="":
                content_like_count=0            
            #형변환
            content_like_counts.append(int(content_like_count))

        #댓글 ID,댓글,댓글 좋아요
        return comment_ids,content_texts,content_like_counts

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