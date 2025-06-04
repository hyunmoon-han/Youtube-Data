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


warnings.filterwarnings('ignore')
src_path = os.path.dirname(os.path.abspath(__file__))

class LG_YOUTUBE_SCRIPT():
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

        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )


        # 결과 저장 데이터프레임
        self.result_df = pd.DataFrame(columns = ["video_id","script","load_dttm"])
        
        #에러 테이블 
        #self.error_df =  pd.DataFrame(columns = ["stddt","log_seq","video_id", "channel_id", "error_flag", "program_name","log_msg"])
        self.error_df =  pd.DataFrame(columns = ["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

        #적재 시간
        self.stddt = datetime.now().strftime("%Y%m%d")
        self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
                

        #키워드 + DB 적재된 영상 ID가져오기
        self.fetch_video_id()
           

    #비디오 영상 하나당 처리 함수
    def script_master_detail(self, video_id):        
        script_text = None
        error_msg = None  
        #try:
            #for id in tqdm(self.video_id_df['video_id']) :
                
        # 자막 다운로드
        download_check = self.download_subtitles(video_id)

        # 자막 읽기 ->다운로드 성공 시
        if download_check == 'S':
            subtitles = self.read_vtt_file(video_id)

            if subtitles is not None:
                full_text = ' '.join(subtitles)
                script_text = ' '.join(full_text.split())  # 줄바꿈, 중복 공백 제거
                read_check = 'S'
                
            else:                
                read_check = 'F'
                error_msg = 'read_error'
        else:
            read_check = 'N'
            error_msg = 'download_error'

        # 결과 저장
        status = 'N' if download_check == 'S' and read_check == 'S' else 'Y'

        df = pd.DataFrame({
            'video_id': [video_id],
            'script': [script_text],
            'error_flag':[status],
            'log_msg':[error_msg],
            'channel_id':[None]
        })
        return df
        # except Exception as e:
        #     return pd.DataFrame([{
        #         'video_id': [video_id],
        #         'script': [script_text],
        #         'error_flag':'Y',
        #         'error_msg':[error_msg],
        #         'channel_id':[channel_id]
        #     }])
            



        

    #실행 함수
    def youtube_crawler_runner(self):

        #스크립트 값이 없는것들만 필터링
        if self.seq_number==1:
            filtered_df = self.video_id_df[self.video_id_df['scriptc_yn'] == 'N']
            video_ids = filtered_df['video_id'].tolist()
        else:
            #에러테이블 돌릴때때
            video_ids = self.input_video_ids
        print('추가해야할 스크립트 작업 수:',len(video_ids))
        if len(video_ids) == 0:
            print('작업할 영상이 없습니다 ')
            return 


        #video_ids = self.video_id_df['video_id'].tolist()
        #channel_ids = self.video_id_df['channel_id'].tolist()
        #scriptc_yns = self.video_id_df['scriptc_yn']

        with ThreadPoolExecutor(max_workers=5) as executor:
            #results = list(executor.map(self.script_master_detail, video_ids,channel_ids))
            #results = list(executor.map(self.script_master_detail, video_ids, channel_ids))
            results = list(executor.map(self.script_master_detail, video_ids))


        df = pd.concat(results, ignore_index=True)
        df['load_dttm'] = self.load_dttm
        df['stddt'] = self.stddt
        df['log_seq'] = self.seq_number
        df['program_name'] = 'youtube_video_script'
        print('총 작업 갯수 :',len(df))
        #컬럼 선정
        result_df_list = ['video_id','script','load_dttm']
        error_df_list = ["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"]
        
        
        #성공한 것만 저장 
        su_df = df[df['error_flag']=='N']
        result_temp_df =su_df[result_df_list]
        
        
        #에러 테이블
        error_temp_df = df[error_df_list]
        

        

        #저장.
        self.result_df = pd.concat([self.result_df, result_temp_df], ignore_index=True) 
        print('작업 성공한 수:',len(self.result_df))
        self.db_saver('youtube_video_script', self.result_df)   
        
        #에러테이블
        self.error_df =  pd.concat([self.error_df, error_temp_df], ignore_index=True)    
        self.db_saver('youtube_error_log', self.error_df)   
        #print('작업 실패한 수 :',len(self.error_df['error_flag']=='Y').sum())
        print('작업 실패한 수 :', (self.error_df['error_flag'] == 'Y').sum())


        #임시 마스터 테이블 컬럼값 변경
        self.master_update()
        print('컬럼값 업데이트 성공')

        #자막 다운로드 파일 삭제.
        self.delete_all_files_in_vtt_path()
        print('자막 파일 삭제 성공')

        #프로시져 호출
        self.call_procedure('update_script')
        print('프로시저 호출 성공')


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
            SELECT DISTINCT video_id, scriptc_yn
            FROM `{self.schema}`.temp_id
        """
        self.video_id_df = pd.read_sql(query, con=conn)

    def download_subtitles(self, video_id):
        # 바탕화면에 youtube_script 폴더 경로
        vtt_path= Path.home() / 'Desktop' / 'youtube_script' 

        vtt_path.mkdir(parents=True, exist_ok=True)  # 폴더가 없으면 생성
        
        # 자막 다운로드 옵션
        ydl_opts = {
            'quiet': True,
            'no_warnings': True ,    # 경고 메시지 무시
            'writeautomaticsub': True,  # 자막 다운로드
            'subtitleslangs': ['ko'],  # 한국어 자막 다운로드
            'skip_download': True,  # 영상 다운로드를 건너뜁니다.
            'outtmpl': str(vtt_path / f"{video_id}.%(ext)s"),  # 파일 이름 형식 (영상ID.vtt)
        }


        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f'https://www.youtube.com/watch?v={video_id}'])

            original_file = vtt_path / f"{video_id}.ko.vtt"
            target_file = vtt_path / f"{video_id}.vtt"

            if original_file.exists():
                shutil.move(str(original_file), str(target_file))
                return "S"
            else:
                return "F"
            
        except Exception:
            return "F"
        
    def read_vtt_file(self, video_id):
        # 바탕화면에 youtube_script 폴더 경로
        
        vtt_path= Path.home() / 'Desktop' / 'youtube_script' /f"{video_id}.vtt"
        
        if not vtt_path.exists():
            print(f"파일 {vtt_path}가 존재하지 않습니다.")
            return None
        
        subtitles = []
        seen = set()
        try:
            with open(vtt_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            for line in lines:
                # 타임코드 및 메타 정보 무시
                if re.match(r"^\d\d:\d\d:\d\d\.\d\d\d -->", line):
                    continue
                if line.strip() == '' or line.startswith('WEBVTT') or line.startswith('Kind:') or line.startswith('Language:'):
                    continue

                # <c> 태그, <00:00:xx.xxx> 시간 태그 제거
                clean_line = re.sub(r'<[^>]+>', '', line).strip()

                # 중복 제거 후 저장
                if clean_line and clean_line not in seen:
                    seen.add(clean_line)
                    subtitles.append(clean_line)

            
            return subtitles    
        except Exception:
            return None

    def log_writter(self, msg):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_content = "[%s topx/naver_api_topx.py] %s  "%(current_time, msg)
        print(log_content)  

    #프로시져 호출하는 함수
    def call_procedure(self, proc_name):
        conn = self.db_connector().raw_connection()
        try:
            cursor = conn.cursor()
            cursor.callproc(proc_name)
            cursor.close()
            conn.commit()
        finally:
            conn.close()  

     #마스터 테이블 업데이트
    def master_update(self):
         # 업데이트할 video_id 목록
        success_ids = self.result_df['video_id'].tolist()

        #업데이트 값 없으면 제외
        if not success_ids:
            return  # 업데이트할 항목 없음

        update_query = text(f"""
            UPDATE `{self.schema}`.temp_id
            SET scriptc_yn = 'Y'
            WHERE video_id IN :video_ids
        """).bindparams(bindparam("video_ids", expanding=True))

        # SQLAlchemy 2.0 방식: Session 사용
        with Session(self.engine) as session:
            session.execute(update_query, {"video_ids": success_ids})
            session.commit()

    #모든 작업이 완료된 후 만들어진 자막 파일 삭제.
    def delete_all_files_in_vtt_path(self):
        vtt_path = Path.home() / 'Desktop' / 'youtube_script'
        
        if vtt_path.exists() and vtt_path.is_dir():
            for file in vtt_path.iterdir():
                if file.is_file():
                    file.unlink()  # 파일 삭제
            print("모든 파일이 삭제되었습니다.")
        else:
            print("경로가 존재하지 않거나 폴더가 아닙니다.")

if __name__ == "__main__":
    topxc = LG_YOUTUBE_SCRIPT(1,[])

    start_time = time.time()
    topxc.youtube_crawler_runner()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")



    
   
   

