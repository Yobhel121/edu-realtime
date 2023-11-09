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

