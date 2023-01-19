select 
    regexp_extract(user_agent, '[^\/]+') as browser
    ,status_code
    ,count(*) as cnt_staus
from server_logs
group by 
    regexp_extract(user_agent, '[^\/]+')
    ,status_code
;

-- 06 order by 
-- 注意需要设置 watermark 才能排序
CREATE TEMPORARY TABLE server_logs2 ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    user_agent STRING,
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    size INT, 
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgentAny}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);


select * from server_logs2  order by log_time;

-- 滚动窗口统计, 统计 1m+15s 数据? 还是 1m 内数据, 统计的实际业务开始和结束时间?
-- TODO 这里如何验证功能正确的? 即UT
select 
    tumble_rowtime(log_time, interval '1' minute) as window_time,
    regexp_extract(user_agent, '[^\/]+') as browser,
    count(*) as cnt_browser
from server_logs2
group by 
    regexp_extract(user_agent, '[^\/]+')
    ,tumble(log_time, interval '1' minute)
order by window_time 
    ,cnt_browser desc
;


-- 07 create (temporary) view 

create view successful_requests as 
select *
from server_logs
where status_code similar to '[2,3][0-9][0-9]';

select * from successful_requests;


-- 08 result write multi table (kafka/hdfs etc...)
CREATE TEMPORARY TABLE server_logs3 ( 
    client_ip       STRING,
    client_identity STRING, 
    userid          STRING, 
    user_agent      STRING,
    log_time        TIMESTAMP(3),
    request_line    STRING, 
    status_code     STRING, 
    size            INT,
    WATERMARK FOR log_time AS log_time - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.user_agent.expression' = '#{Internet.userAgentAny}',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',
  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'
);

-- 每5分钟查看浏览器状态代码数量; 此外, 以分区的parquet文件格式存储每小时信息, 用于历史分析;
-- 正常是写两个sql处理逻辑, 这里使用 statement sets 功能, 多路复用由Flink优化为单个查询;
create temporary table realtime_aggregations (
    browser string,
    status_code string,
    end_time timestamp(3),
    requests bigint not null
) with (
    'connector' = 'kafka',
    'topic' = 'browser-status-codes',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'browser-counts',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'avro'
);


create temporary table offline_datawarehouse(
    browser string,
    status_code string,
    `dt` string,
    `hour` string, 
    requests bigint not null 
) partitioned by (dt, `hour`) with ( -- todo 注意使用 partitioned; hour为关键词
    'connector' = 'filesystem',
    'path' = 'hdfs://node1:9820/my-bucket/browser-into',
    'sink.partition-commit.trigger' = 'partition-time',
    'format' = 'parquet'
);

create temporary view browsers as 
select 
    regexp_extract(user_agent, '[^\/]+') as browser,
    status_code,
    log_time
from server_logs3;


begin statement set;


insert into realtime_aggregations 
select 
    browser,
    status_code,
    tumble_rowtime(log_time, interval '5' minute) as end_time,
    count(*) requests 
from browsers
group by 
    browser,
    status_code,
    tumble(log_time, interval '5' minute) -- TODO 为什么这里不加 rowtime ?
;

insert into offline_datawarehouse
select 
    browser,
    status_code,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, interval '1' hour), 'yyyy-MM-dd') as dt,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, interval '1' hour), 'HH') as `hour`,
    count(*) as requests
from browsers
group by 
    browser,
    status_code,
    tumble(log_time, interval '1' hour)
;
END;


-- test 单个作业执行, 解决jar包等问题
insert into offline_datawarehouse
select 
    browser,
    status_code,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, interval '1' seconds), 'yyyy-MM-dd') as dt,
    DATE_FORMAT(TUMBLE_ROWTIME(log_time, interval '1' seconds), 'HH') as `hour`,
    count(*) as requests
from browsers
group by 
    browser,
    status_code,
    tumble(log_time, interval '1' seconds)
;

-- 不使用 partitioned 方式试试
create temporary table hdfs_test(
    browser string,
    status_code string,
    `dt` string,
    `hour` string, 
    requests bigint not null 
) with (
    'connector' = 'filesystem',
    -- 'path' = 'hdfs://node1:9820/tmp/hdfs_test/1.txt',
    'path' = '/tmp/1.txt',
    'format' = 'csv'
);

insert into hdfs_test
select '1','1','1','1',1
;
x


-- 09 时区转换时间戳
CREATE TABLE iot_status ( 
    device_ip       STRING,
    device_timezone STRING,
    iot_timestamp   TIMESTAMP(3),
    status_code     STRING
) WITH (
  'connector' = 'faker', 
  'fields.device_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.device_timezone.expression' =  '#{regexify ''(America\/Los_Angeles|Europe\/Rome|Europe\/London|Australia\/Sydney){1}''}',
  'fields.iot_timestamp.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.status_code.expression' = '#{regexify ''(OK|KO|WARNING){1}''}',
  'rows-per-second' = '3'
);


select
    device_ip,
    device_timezone,
    iot_timestamp,
    convert_tz(cast(iot_timestamp as string), device_timezone, 'UTC') as iot_timestamp_utc,
    status_code
from iot_status
;    