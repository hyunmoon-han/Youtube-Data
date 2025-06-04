import yt_dlp
# import re
import pandas as pd

# # # 비디오 ID

# # #776gkhBdpBI
# # #데이터 가져오는 함수 
# def search_youtube():
    
#     video_url = 'https://www.youtube.com/watch?v=1QTDrcOnEmc'
#     ydl_opts = {'quiet': True,'no_warnings': True }
    
#     with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        
#         info = ydl.extract_info(video_url, download=False)

#         A=  info.get('description','정보없음')
#         B =  info.get('tags', '정보 없음')
#         print('설명 :',A)
#         print()
#         print('태그 :',B)

# search_youtube()
                   
#채널 정보 저장하는 프로그램램    
# # search_youtube()
# import time
# import pandas as pd
# import numpy as np
# import re
# import json
# from datetime import datetime, timedelta
# import awswrangler as wr
# from selenium.webdriver.support import expected_conditions as EC
# import os
# import warnings
# import re
# from sqlalchemy import create_engine
# from concurrent.futures import ThreadPoolExecutor
# import yt_dlp
# from sqlalchemy import text
# from sqlalchemy.orm import Session
# from sqlalchemy import text, bindparam
# import unicodedata

# warnings.filterwarnings('ignore')
# src_path = os.path.dirname(os.path.abspath(__file__))

# class LG_YOUBUTE_DETAIL_NUM():
#     def __init__(self):

#         # # DB 연결 정보 load
#         with open(os.path.join(src_path, "..","..", "keys","db_keys.json"), "rb") as f:
#             db_keys = json.load(f)

#         self.user = db_keys["user"]
#         self.password = db_keys["password"]
#         self.host = db_keys["host"]
#         self.db = db_keys["db"]
#         self.schema = db_keys["schema"]
#         self.port = db_keys["port"]


#         # 결과 저장 데이터프레임(댓글)
#         #self.comment_temp_df = pd.DataFrame(columns = ["comment_id","video_id","channel_id","comment_total_count","comment_text","content_like_count","load_dttm"])

#         #self.video_num_temp_df = pd.DataFrame(columns = ["load_dttm","video_id","channel_id","view_count","like_count","comment_count"])

#         #self.error_df =  pd.DataFrame(columns = ["stddt","video_id", "channel_id", "error_flag", "program_name"])
#         self.error_df =  pd.DataFrame(columns = ["stddt",'log_seq',"video_id", "channel_id", "error_flag", "program_name","log_msg","load_dttm"])

#         #self.load_dttm = datetime.now().strftime("%Y%m%d%H")
#         self.load_dttm = datetime.now().strftime("%Y%m%d%H%M")
#         self.stddt = datetime.now().strftime("%Y%m%d")

                
#         self.engine = create_engine(
#             f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
#         )

#         self.fetch_video_id()


#     #영상 마스터에서 고정값 정보가 필요한 영상 ID 가져오기
#     def fetch_video_id(self):
#         conn = self.db_connector()  # SQLAlchemy engine 또는 connection
#         query = f"""
#             SELECT video_id 
#             FROM `{self.schema}`.temp_master
#         """
#         self.video_df = pd.read_sql(query, con=conn)
    

#     #메인 실행함수
#     def youtube_crawler_runner(self):
        
#         video_ids = self.video_df['video_id'].tolist()
        
#         with ThreadPoolExecutor(max_workers=5) as executor:
#             results = list(executor.map(self.master_detail, video_ids))

#         # 결과를 DataFrame으로 변환
#         df = pd.DataFrame(results)
#         # 성공한 작업과 실패한 작업을 구분
#         if len(df[(df['channel_id'].isnull()) | (df['channel_url'].isnull())]) >= 1:
#             return 0
#         else:
#             self.db_saver('temp_a',df)


             
#     def master_detail(self,video_id):
#         url = f'https://www.youtube.com/watch?v={video_id}'
#         ydl_opts = {'quiet': True,'no_warnings': True }
#         with yt_dlp.YoutubeDL(ydl_opts) as ydl:

#             info = ydl.extract_info(url, download=False)
            
#             # 필요한 정보를 dict로 반환
#             return {
#                 'video_id': video_id,
#                 'channel_id': info.get('channel_id',None),  
#                 'channel_url': info.get('channel_url',None)
#             }

      


    

#     def db_connector(self):
#         connection_url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
#         conn = create_engine(connection_url)
#         return conn

#     def db_saver(self, target_db_table, dataframe):
#         # DB 연결 객체
#         conn = self.db_connector()
#         # DB 데이터 적재
#         dataframe.to_sql(
#             name=target_db_table,
#             con=conn,
#             schema=self.schema,
#             if_exists="append",
#             index=False
#         )
#         conn.dispose()
    


# if __name__ == "__main__":

#     topxc = LG_YOUBUTE_DETAIL_NUM()
#     start_time = time.time()
#     topxc.youtube_crawler_runner()
#     end_time = time.time()
#     elapsed_time = end_time - start_time
#     print(f"\n⏱ 실행 시간: {elapsed_time:.2f}초")
import re

 #데이터 가져오는 함수 
def search_youtube(video_id):
    
    url = f'https://www.youtube.com/watch?v={video_id}'
    ydl_opts = {'quiet': True,'no_warnings': True }
    log_msg=None
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:        
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

        df = pd.DataFrame([{
            'video_id': video_id,
            'description': info.get('description'),
            'tags': info.get('tags', '정보 없음')
        }])
        return df
    #영상 설명부분 추출
# def description_script(description):        
            
#     if '#' in description :
#         #등장하는 앞 부분까지 커트
#         description= description.split('#')[0]        
    
#     return description

    return cleaned
def description_script(text):

    return re.sub(r'(?:#\S+)+', '', text).strip()
    
#태그정보 병합
def merge_tags(description, tags):
    # description에서 해시태그 추출
    hashtags = re.findall(r'#(\w+)', description)
    
    if tags is None:
        tags = []
    
    # 중복 제거 후 병합
    merged_tags = list(set(hashtags + tags))
    return merged_tags

def moon():

    A=search_youtube("1QTDrcOnEmc")
    df =A
    
    #태그정보 병합
    print('함수 전 시작 값 확인 : ',df['tags'].iloc[0])
    df['tags'] = df.apply(lambda row: merge_tags(row['description'], row['tags']), axis=1)
    df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    df['description'] = df['description'].apply(description_script)

    print("description:",df['description'].iloc[0])
    print("tags:",df['tags'].iloc[0])

moon()
