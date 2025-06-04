#사용자 입력 테이블
CREATE TABLE `input_keyword` (
  `keyword` varchar(30) DEFAULT NULL COMMENT '키워드',
  `del_yn` varchar(2) DEFAULT 'N' COMMENT '삭제여부(Y/N)',
  `load_dttm` varchar(15) DEFAULT NULL COMMENT '적재일자'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#영상마스터_temp
CREATE TABLE `temp_id` (
  `video_id` varchar(100) NOT NULL COMMENT '영상ID',
  `keyword` varchar(100) DEFAULT NULL COMMENT '키워드',
  `original_url` varchar(255) DEFAULT NULL COMMENT '영상URL',
  `pick_yn` varchar(2) DEFAULT 'N' COMMENT '고정값유무(Y/N)',
  `load_dttm` varchar(12) DEFAULT NULL COMMENT '적재일자',
  `scriptc_yn` varchar(2) DEFAULT 'N' COMMENT '스크립트 유뮤(Y/N)',
  PRIMARY KEY (`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#영상마스터
CREATE TABLE `temp_master` (
  `video_id` varchar(100) NOT NULL COMMENT '영상ID',
  `keyword` varchar(100) DEFAULT NULL COMMENT '키워드',
  `channel_id` varchar(100) DEFAULT NULL COMMENT '채널ID',
  `channel_nm` varchar(100) DEFAULT NULL COMMENT '채널이름',
  `channel_url` varchar(255) DEFAULT NULL COMMENT '채널URL',
  `title` varchar(500) DEFAULT NULL COMMENT '제목',
  `description` text DEFAULT NULL COMMENT '설명',
  `thumbnail` varchar(255) DEFAULT NULL COMMENT '썸네일정보',
  `tags` text DEFAULT NULL COMMENT '태그',
  `categories` varchar(100) DEFAULT NULL COMMENT '카테고리',
  `duration_string` varchar(20) DEFAULT NULL COMMENT '영상길이(00:00:00)',
  `duration` int(11) DEFAULT NULL COMMENT '영상길이(초)',
  `original_url` varchar(255) DEFAULT NULL COMMENT '영상URL',
  `language` varchar(10) DEFAULT NULL COMMENT '언어',
  `upload_date` varchar(12) DEFAULT NULL COMMENT '업로드일자',
  `load_dttm` varchar(12) DEFAULT NULL COMMENT '적재일자',
  `add_yn` varchar(1) DEFAULT 'N' COMMENT '유료 광고 여부',
  `channel_follower_count` int(11) DEFAULT NULL COMMENT '구독자 수',
  `script` longtext DEFAULT NULL COMMENT '스크립트',
  PRIMARY KEY (`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#에러테이블
CREATE TABLE `youtube_error_log` (
  `stddt` varchar(8) NOT NULL COMMENT '기준일자',
  `log_seq` int(11) NOT NULL DEFAULT 1 COMMENT '순번',
  `video_id` varchar(50) DEFAULT NULL COMMENT '영상ID',
  `channel_id` varchar(50) DEFAULT NULL COMMENT '채널ID',
  `error_flag` varchar(100) DEFAULT NULL COMMENT 'Y : 에러 발생 N : 정상 구동',
  `program_name` varchar(50) DEFAULT NULL COMMENT '프로그램 이름',
  `log_msg` text DEFAULT NULL COMMENT '에러내용',
  `load_dttm` varchar(12) DEFAULT NULL COMMENT '적재일자'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#댓글
CREATE TABLE `youtube_video_comment_dd` (
  `comment_id` varchar(35) DEFAULT NULL COMMENT '댓글ID',
  `video_id` varchar(30) DEFAULT NULL COMMENT '영상ID',
  `channel_id` varchar(30) DEFAULT NULL COMMENT '채널ID',
  `comment_text` text DEFAULT NULL COMMENT '댓글내용',
  `content_like_count` int(11) DEFAULT NULL COMMENT '댓글좋아요 수',
  `load_dttm` varchar(15) DEFAULT NULL COMMENT '적재일자'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#영상 반응(일)
CREATE TABLE `youtube_video_dd` (
  `load_dttm` varchar(15) NOT NULL COMMENT '적재일자',
  `video_id` varchar(16) NOT NULL COMMENT '영상 ID',
  `channel_id` varchar(50) DEFAULT NULL COMMENT '채널ID',
  `view_count` int(11) DEFAULT NULL COMMENT '조회수',
  `like_count` int(11) DEFAULT NULL COMMENT '좋아요 수',
  `comment_count` int(11) DEFAULT NULL COMMENT '댓글수',
  PRIMARY KEY (`load_dttm`,`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='유튜브_영상_일내역';


#영상 스크립트 정보보
CREATE TABLE `youtube_video_script` (
  `video_id` varchar(30) DEFAULT NULL COMMENT '영상ID',
  `script` longtext DEFAULT NULL COMMENT '자막',
  `load_dttm` varchar(15) DEFAULT NULL COMMENT '적재일자'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

#프로시져
CREATE DEFINER=`root`@`%` PROCEDURE `lg_youtube`.`update_script`()
begin
    UPDATE lg_youtube.temp_master AS m				
      JOIN lg_youtube.youtube_video_script AS s 
        ON m.video_id = s.video_id
       SET m.script = s.script
     where 1=1
       and m.script IS NULL
       AND s.script IS NOT NULL; -- 스크립트가 존재하는 경우만 업데이트
END