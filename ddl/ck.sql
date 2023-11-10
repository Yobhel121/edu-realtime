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