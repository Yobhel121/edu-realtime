drop table if exists dws_traffic_source_keyword_page_view_window;
create table if not exists dws_traffic_source_keyword_page_view_window
(
    stt           DateTime,
    edt           DateTime,
    source        String,
    keyword       String,
    keyword_count UInt64,
    ts            UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, source, keyword);

drop table if exists dws_traffic_vc_source_ar_is_new_page_view_window;
create table dws_traffic_vc_source_ar_is_new_page_view_window(
     stt                 DateTime,
     edt                 DateTime,
     version_code        String,
     source_id           String,
     source_name         String,
     ar                  String,
     province_name       String,
     is_new              String,
     uv_count            UInt64,
     total_session_count UInt64,
     page_view_count     UInt64,
     total_during_time   UInt64,
     jump_session_count  UInt64,
     ts                  UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, version_code, source_id, source_name, ar, province_name, is_new);

drop table if exists dws_traffic_page_view_window;
create table dws_traffic_page_view_window(
     stt DateTime,
     edt DateTime,
     home_uv_count UInt64,
     list_uv_count UInt64,
     detail_uv_count UInt64,
     ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_learn_chapter_play_window;
create table dws_learn_chapter_play_window(
    stt DateTime,
    edt DateTime,
    chapter_id String,
    chapter_name String,
    play_count UInt64,
    play_total_sec UInt64,
    play_uu_count UInt64,
    ts UInt64
) engine ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, chapter_id, chapter_name);

drop table if exists dws_user_login_window;
create table dws_user_login_window(
    stt DateTime,
    edt DateTime,
    back_count UInt64,
    uv_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_user_register_window;
create table dws_user_register_window(
    stt DateTime,
    edt DateTime,
    register_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_trade_cart_add_window;
create table dws_trade_cart_add_window(
    stt DateTime,
    edt DateTime,
    cart_add_uv_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_trade_order_window;
create table dws_trade_order_window(
    stt DateTime,
    edt DateTime,
    order_uv_count UInt64,
    new_order_user_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_trade_pay_suc_window;
create table dws_trade_pay_suc_window(
    stt DateTime,
    edt DateTime,
    pay_suc_uv_count UInt64,
    pay_suc_new_user_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt);

drop table if exists dws_trade_course_order_window;
create table dws_trade_course_order_window(
    stt DateTime,
    edt DateTime,
    course_id String,
    course_name String,
    subject_id String,
    subject_name String,
    category_id String,
    category_name String,
    order_total_amount Decimal(38, 20),
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, course_id, course_name);

drop table if exists dws_trade_source_order_window;
create table dws_trade_source_order_window(
    stt DateTime,
    edt DateTime,
    source_id String,
    source_name String,
    order_total_amount Decimal(38, 20),
    order_uu_count UInt64,
    order_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, source_id, source_name);

drop table if exists dws_trade_province_order_window;
create table dws_trade_province_order_window(
    stt DateTime,
    edt DateTime,
    province_id String,
    province_name String,
    order_total_amount Decimal(38, 20),
    order_uu_count UInt64,
    order_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, province_id, province_name);

drop table if exists dws_interaction_course_review_window;
create table dws_interaction_course_review_window(
    stt DateTime,
    edt DateTime,
    course_id String,
    course_name String,
    review_total_stars UInt64,
    review_user_count UInt64,
    good_review_user_count UInt64,
    ts UInt64
) engine = ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, course_id, course_name);

drop table if exists dws_examination_paper_exam_window;
create table dws_examination_paper_exam_window(
    stt DateTime,
    edt DateTime,
    paper_id String,
    paper_title String,
    course_id String,
    course_name String,
    exam_taken_count UInt64,
    exam_total_score UInt64,
    exam_total_during_sec UInt64,
    ts UInt64
) engine ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, paper_id, paper_title);

drop table if exists dws_examination_paper_score_duration_exam_window;
create table dws_examination_paper_score_duration_exam_window(
    stt DateTime,
    edt DateTime,
    paper_id String,
    paper_title String,
    score_duration String,
    user_count UInt64,
    ts UInt64
) engine ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, paper_id, paper_title, score_duration);

drop table if exists dws_examination_question_answer_window;
create table dws_examination_question_answer_window(
    stt DateTime,
    edt DateTime,
    question_id String,
    question_txt String,
    correct_answer_count UInt64,
    answer_count UInt64,
    ts UInt64
) engine ReplacingMergeTree(ts)
	partition by toYYYYMMDD(stt)
	order by(stt, edt, question_id, question_txt);