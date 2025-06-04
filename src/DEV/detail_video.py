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
from sqlalchemy import text
from tqdm import tqdm
from sqlalchemy.orm import Session
from sqlalchemy import text, bindparam

warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUBUTE_DETAIL():
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
        self.result_df = pd.DataFrame(columns = ["video_id","channel_id","title","channel_nm","channel_img_url","tags","add_yn","thumbnail", "description","script","chapter","lang","upload_date","load_dttm"])

        #에러 테이블 
        self.error_fail_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])
        self.error_ssuccess_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])
        

        #적재 시간
        self.load_dttm = datetime.now().strftime("%Y%m%d%H")


        self.landing_page_loader()    
        
        self.driver.set_page_load_timeout(5)


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
        
        time.sleep(3)

    #스크립트 정보 추출        
    def script_crawling(self):

        #스크립트 클릭
        self.driver.find_element(By.XPATH,'//*[@id="primary-button"]/ytd-button-renderer/yt-button-shape/button/yt-touch-feedback-shape/div').click()
        # element = WebDriverWait(self.driver, 10).until(
        #                 EC.presence_of_element_located((By.XPATH, '//*[@id="primary-button"]/ytd-button-renderer/yt-button-shape/button/yt-touch-feedback-shape/div'))
        #             )
        # element.click()
        time.sleep(1.5)
        # 'segments-container'가 로딩될 때까지 기다림
        container= WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, 'segments-container'))
        )
        # 세그먼트 최상위 부모값 로딩
        #container = self.driver.find_element(By.ID, 'segments-container')
        time.sleep(1.5)
        
        # 그 안에 해당하는 자식 태그 로딩
        children = container.find_elements(By.XPATH, './*')
        time.sleep(1.5)
        
        # 결과값 저장 변수
        transcript_data = {}
        
        # 헤더값이 없을 경우 문구 설정
        current_header = "헤더없음"
        
        for child in children:
            tag_name = child.tag_name
        
            # 헤더값 추출
            if tag_name == 'ytd-transcript-section-header-renderer':
                header_text = child.text.strip()
                if not header_text:
                    current_header = "헤더없음"
                else:
                    current_header = header_text
        
                if current_header not in transcript_data:
                    transcript_data[current_header] = []
            #일반 스크립트 추출
            elif tag_name == 'ytd-transcript-segment-renderer':
                time_text = child.find_element(By.CSS_SELECTOR, '.segment-timestamp').text.strip()
                text = child.find_element(By.CSS_SELECTOR, '.segment-text').text.strip()
                transcript_data.setdefault(current_header, []).append({
                    "time": time_text,
                    "text": text
                })
        print('스크립트 성공')
        
        return transcript_data
    
    #챕터 정보 추출
    def chapter_scrip(self):

        json_result = '정보없음'   
        #챕터 있는지 확인 후 추출
        chip_wrappers = self.driver.find_elements(By.CLASS_NAME, "ytChipBarViewModelChipWrapper")
        time.sleep(1.5)

        if len(chip_wrappers) != 1:
                                
            # "챕터" 텍스트를 포함한 요소 클릭하기
            for chip in chip_wrappers:
                # 텍스트 확인 (공백 제거 후)
                chip_text = chip.text.strip()

                # '챕터'라는 텍스트가 포함된 요소 찾기
                if "챕터" in chip_text:
                    chip.click()
                    
                    # 결과 리스트 초기화
                    result_list = []
                    
                    # 모든 details 요소 찾기
                    details_divs = self.driver.find_elements(By.ID, "details")
                    time.sleep(1.5)
                    
                    for details_div in details_divs:
                        # 텍스트 추출 후 줄바꿈/공백 제거
                        full_text = details_div.text.strip()
                    
                        # 빈 값이면 스킵
                        if not full_text:
                            continue
                            
                        splitted = full_text.split()
                    
                        # 시간과 텍스트 분리 (시간 포맷이 있는 값 탐색)
                        time_content = ""
                        text_content = ""
                    
                        for part in splitted:
                            if ":" in part:
                                time_content = part
                            else:
                                text_content += part + " "
                    
                        # 결과 딕셔너리에 저장
                        result = {
                            "time": time_content.strip(),
                            "text": text_content.strip()
                        }
                        result_list.append(result)
                    
                    # JSON 형식 출력
                    json_result = json.dumps(result_list, ensure_ascii=False)    
                    break  # 첫 번째 '챕터'만 클릭하면 되므로 반복문 종료


        print('챕터 성공')
        return json_result    
    
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
    
    #썸네일정보 + 영상ID 추출하는 함수
    def thumbnail_video_id(self):
        # 현재 유튜브 영상 URL
        video_url = self.driver.current_url

        # 영상 ID 추출
        parsed = urlparse(video_url)
        video_id = parse_qs(parsed.query).get('v', [None])[0]

        if video_id:
            thumbnail = f'https://img.youtube.com/vi/{video_id}/hqdefault.jpg'
            #json으로 변환
            data = {
                "url": thumbnail
            }
            thumbnail=json.dumps(data)
        #영상ID , 썸네일정보 리턴
        print('영상ID통과')
        return video_id,thumbnail
    
    

    #메인 실행함수
    def youtube_crawler_runner(self):
        #DB에 적재된 영상 목록 조회
        self.fetch_video_id()
        
        try :
            for k in tqdm(range(len(self.video_df))) :
                
                check_yn_temp = self.video_df.loc[k, 'pick_yn']
                        
                #영상 마스터에 고정값 유무 확인 후 없을시 프로그램 시작.
                if check_yn_temp=='N':
                    #더블체크용으로 video_id 
                    #video_id_temp = self.video_df.loc[k, 'video_id']

                    url_temp = self.video_df.loc[k, 'url']

                    
                    self.driver.get(url_temp)
                    
                    
                    #더보기 클릭
                    # self.driver.find_element(By.XPATH,'//*[@id="expand"]').click()        
                    # time.sleep(1.5)  
                    element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="expand"]'))
                    )
                    element.click()

                    #영상 제목
                    title = self.driver.find_element(By.XPATH, '//*[@id="title"]/h1/yt-formatted-string').text
                    time.sleep(1.5)  
                    
                    #썸네일정보 + 영상ID 
                    video_id,thumbnail = self.thumbnail_video_id()

                    #영상 설명 부분
                    description = self.description_script()

                    #채널ID,이름 추출
                    channel_id,channel_nm  = self.chnnl_script()

                    # #채널구독자 카운트 추출
                    # channel_follower_count = self.channel_follower_count_script()
                    # channel_follower_count =int(channel_follower_count)

                    # #영상 좋아요 카운트 추출
                    # like_count = self.like_count()
                    # like_count = int(like_count)

                    #영상 조회수 + 업로드 일자 
                    view_count,upload_date = self.view_count_upload_script()
                    #view_count = int(view_count)

                    #영상 태그정보
                    tags = self.tag_script()

                    #유로광고여부 체크
                    add_yn = self.is_paid_promotion(description)
                    if add_yn == True:
                        add_yn = 'Y'
                    else:
                        add_yn = 'N'

                    #채널 썸네일 이미지 추출
                    chnnl_img_url = self.chnnl_img_url()
                    

                    #페이지 언어.
                    page_lang = self.driver.find_element(By.TAG_NAME, 'html').get_attribute('lang')
                    time.sleep(2)

                    #스크립트 수집
                    script = self.script_crawling()
                    time.sleep(2)

                    
                    #챕터정보 수집
                    chapter = self.chapter_scrip()

                    #댓글정보는 변동성 있을때 다른부분에서 수집예정
                    # #댓글 - 임시로 페이지 한번 내리고 시작작 
                    # self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                    # time.sleep(3)
                    # comment_count = self.driver.find_element(By.XPATH,'//*[@id="leading-section"]//h2[@id="count"]//span[2]').text
                    


                    #데이터프레임 생성 
                    df = pd.DataFrame({
                        'video_id': [video_id], 
                        'channel_id': [channel_id],
                        'title': [title],
                        'channel_nm': [channel_nm],
                        'channel_img_url': [chnnl_img_url],         
                        'tags': [tags],
                        'add_yn': [add_yn],                                 
                        'thumbnail': [thumbnail],
                        'description': [description],
                        'script': [json.dumps(script, ensure_ascii=False)], 
                        'chapter': [json.dumps(chapter, ensure_ascii=False)],
                        'lang': [page_lang],
                        'upload_date': [upload_date],
                        'load_dttm':[self.load_dttm]
                    })
                    
                    self.result_df = pd.concat([self.result_df, df], ignore_index=True)

        except Exception  as err_msg:
                print(err_msg)
                #error_msg = str(err_msg)  
                #에러 발생시 프레임에 적재된 부분을 제외한 부분 에러표시시
                error_msg = str(error_msg)      
                success_ids = self.result_df['video_id'].tolist()

                filter_df = self.video_df[~self.video_df['video_id'].isin(success_ids)].reset_index(drop=True)
                filter_df = filter_df[filter_df['pick_yn'] == 'N'].reset_index(drop=True)


                chnnl_df = self.result_df[self.result_df['video_id'].isin(filter_df['video_id'])]
                channel_ids = chnnl_df['channel_id'].tolist()
                print(channel_ids,len(channel_ids))
                print(len(filter_df['video_id']))
                # 에러 로그 생성 (실패한 데이터 기록)
                error_df_fail_temp =pd.DataFrame({
                    'stddt': [self.load_dttm]* len(filter_df['video_id']),
                    'video_id': filter_df['video_id'].tolist(),
                    'channel_id': [None] * len(filter_df['video_id']),
                    'error_flag': ['Y'] * len(filter_df['video_id']),
                    'program_name': ['video_detail'] * len(filter_df['video_id']),
                    'log_msg' : [error_msg] * len(filter_df['video_id'])
                })
                print('성공?')
                self.error_fail_df = pd.concat([self.error_fail_df, error_df_fail_temp], ignore_index=True)
                #에러 로그 추가
                self.db_saver('error_log', self.error_fail_df)    
        finally:

            #성공적인 데이터는 `error_flag='N'`으로 항상 저장
            error_ssuccess_df_temp =pd.DataFrame({
                    'stddt': [self.load_dttm]* len(self.result_df['video_id']),
                    'video_id': self.result_df['video_id'],
                    'channel_id': self.result_df['channel_id'],
                    'error_flag': ['N'] * len(self.result_df['video_id']),
                    'program_name': 'video_detail',
                    'log_msg' : [None] * len(self.result_df['video_id'])
                })
            self.error_ssuccess_df = pd.concat([self.error_ssuccess_df, error_ssuccess_df_temp], ignore_index=True)
            #에러 로그 추가
            self.db_saver('error_log', self.error_ssuccess_df)    

            #DB추가
            self.db_saver('video_detail',self.result_df)

            #마스터 테이블 업데이트
            self.master_update()
        

    #영상 설명부분 추출
    def description_script(self):
        # 영상 설명 부분 가져오기
        description = self.driver.find_element(By.XPATH,'//*[@id="description-inline-expander"]/yt-attributed-string').text
        time.sleep(2)
        if '#' in description :
            #등장하는 앞 부분까지 커트
            description= description.split('#')[0]        
        print('설명 통과')
        return description
        
    
    #채널정보 추출
    def chnnl_script(self):
        #채널 URL가져오기.
        channel_url = self.driver.find_element(By.XPATH,'//*[@id="owner"]/ytd-video-owner-renderer/a').get_attribute("href")                                    
        time.sleep(1.5)
               
        #채널 ID
        match = re.search(r'/(@[\w\d_]+)', channel_url)
        if match:
            channel_id = match.group(1)
        #채널이름
        channel_nm = self.driver.find_element(By.XPATH,'//*[@id="text"]/a').text                
        time.sleep(1.5)
        print('채널정보 통과')    
        return channel_id,channel_nm   
    
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

    #유로광고체크함수
    def is_paid_promotion(self,description):
        #유로광고 배너이미지 확인
        has_paid_content = False
        #설명 부분에 광고 확인
        has_ad = False
        #배너 부분 광고 확인
        has_merch = False

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
        
        # 1.화면 첫 번째 광고 표시 확인
        elements = self.driver.find_elements(By.CLASS_NAME, "ytp-paid-content-overlay")
        time.sleep(1.5)
        if elements:
            text = elements[0].text.strip()
            if text:
                has_paid_content = True

        # 2.영상 설명 부분에 광고 문구 확인.
        if not any(neg in description for neg in negation_keywords):
            if any(ad in description for ad in ad_keywords):
                has_ad = True



        #3.타이틀 배너에 광고 문구 확인.
        merch_elements = self.driver.find_elements(By.XPATH, '//yt-formatted-string[@id="title" and contains(@class, "ytd-merch-shelf-renderer")]')
        time.sleep(1.5)
        if merch_elements:
            has_merch = True

        #하나라도 포함된다면 , 광고 
        has_any_ad = has_ad or has_paid_content or has_merch
        if has_any_ad:
            print("이 영상은 광고를 포함하고 있습니다.")
        else:
            print("이 영상은 광고를 포함하지 않습니다.")

        print('광고 통과')
        return has_any_ad  

    #태그정보 추출(배너 태그 + 본문 태그)
    def tag_script(self):
        video_tags = set()  # 중복 제거용 set 사용

        # 태그 정보 있을 때
        info_containers = self.driver.find_elements(By.ID, 'info-container')
        time.sleep(1.5)
        if info_containers:
            info_container = info_containers[0]
            a_elements = info_container.find_elements(By.TAG_NAME, 'a')

            for a in a_elements:
                text = a.text.strip()
                if text:
                    video_tags.add(text)

        # 본문(설명)에 태그 정보 있을 때
        target_color = "color: rgb(6, 95, 212);"
        spans = self.driver.find_elements(By.XPATH, '//*[@id="description-inline-expander"]/yt-attributed-string/span/span')
        time.sleep(1.5)
        for span in spans:
            style = span.get_attribute("style")
            if style and target_color in style:
                if '#' in span.text:
                    tag_text = span.text.replace('#', '').strip()
                    if tag_text:
                        video_tags.add(tag_text)

        # 최종 결과를 리스트로 변환
        tags = list(video_tags)
        #tags = [tag.replace("#", "").strip() for tag in tags]
        clean_tags = ','.join(tag.replace("#", "").strip() for tag in tags)
        
        print('태그정보 통과')
        return clean_tags              

    #조회수 + 업로드 일자 추출.
    def view_count_upload_script(self) :
        spans = self.driver.find_elements(By.CSS_SELECTOR, 'span.bold[style-target="bold"]')
        time.sleep(1.5)
        for span in spans:
            text = span.text.strip()
            # 업로드 일자 (날짜) 추출
            if '조회수' in text :  # 텍스트가 비어있지 않은 경우
                match = re.search(r'(\d{1,3}(?:,\d{3})*)', text)
                if match:
                    view_count = match.group(1).replace(',', '')  # 콤마 제거하고 숫자만 가져오기
                      
            elif '.' in text and '조회수' not in text and text:  # 텍스트가 비어있지 않은 경우
                # 날짜 형식: yyyy. mm. dd. 에서 yyyyMMdd로 변환
                date_parts = text.replace('.', '').split()  # '.' 제거하고, 공백으로 분리
                year = date_parts[0]
                month = date_parts[1].zfill(2)  # 1자리인 경우 0을 추가
                day = date_parts[2].zfill(2)    # 1자리인 경우 0을 추가        
                upload_date = f"{year}{month}{day}"  # yyyyMMdd 형식

                #Unix 타임스탬
                # dt = datetime.strptime(upload_date, "%Y%m%d")
                # timestamp = int(dt.timestamp())
                # print('일자 :',upload_date)
                # print('timestamp:',timestamp)
        print('조회수+업로드일자 통과')
        return view_count,upload_date      

    #채널 썸네일 이미지 추출출
    def chnnl_img_url(self):
        # 채널 썸네일 이미지 요소 대기 후 찾기
        wait = WebDriverWait(self.driver, 10)
        img = wait.until(EC.presence_of_element_located((
            By.CSS_SELECTOR,
            "a.yt-simple-endpoint.ytd-video-owner-renderer yt-img-shadow#avatar img#img"
        )))

        # src 추출
        chnnl_img_url = img.get_attribute("src")  
        print('채널 썸네일 이미지 추출')      
        return chnnl_img_url    
    
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
    
    #영상 마스터에서 고정값 정보가 필요한 영상 ID 가져오기
    def fetch_video_id(self):
        conn = self.db_connector()  # SQLAlchemy engine 또는 connection
        query = f"""
            SELECT video_id ,pick_yn, url
            FROM `{self.schema}`.video_master
        """
        self.video_df = pd.read_sql(query, con=conn)

    #마스터 테이블 업데이트트
    def master_update(self):
         # 업데이트할 video_id 목록
        success_ids = self.result_df['video_id'].tolist()

        #업데이트 값 없으면 제외
        if not success_ids:
            return  # 업데이트할 항목 없음

        update_query = text(f"""
            UPDATE `{self.schema}`.video_master
            SET pick_yn = 'Y'
            WHERE video_id IN :video_ids
        """).bindparams(bindparam("video_ids", expanding=True))

        # SQLAlchemy 2.0 방식: Session 사용
        with Session(self.engine) as session:
            session.execute(update_query, {"video_ids": success_ids})
            session.commit()
            
if __name__ == "__main__":

    topxc = LG_YOUBUTE_DETAIL()
    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")