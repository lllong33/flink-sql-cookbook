
/*
KR


*/
-- 01 聚合时间序列数据
-- 计算distinct 每分钟看到的 ip 地址数;
/*
tumble 用于计算时间窗口的开始和结束时间，它接收两个参数，第一个参数是时间属性，第二个参数是时间窗口的大小。
*/
CREATE TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    request_line STRING, 
    status_code STRING, 
    log_time AS PROCTIME() -- 如果日志没有时间, 手动指定计算列做当前系统时间
) WITH (
  'connector' = 'faker', 
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '-',
  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);

select 
  window_start,
  window_end,
  count(distinct client_ip) as ip_addresses 
from table(
  tumble(table server_logs,  descriptor(log_time), interval '1' minute)
)
group by window_start, window_end;

-- TODO 这里 tumble 使用方式有点新, table, time, interval 三个参数, 会返回start和end time


-- 02 watermark
-- tumble 提供时间序列聚合简化;
/* exists bug
[ERROR] Could not execute SQL statement. Reason:
java.lang.RuntimeException: Unable to resolve #{dr_who.the_doctors} directive.
*/
CREATE TABLE doctor_sightings (
  doctor        STRING,
  sighting_time TIMESTAMP(3),
  WATERMARK FOR sighting_time AS sighting_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker', 
  'fields.doctor.expression' = '#{dr_who.the_doctors}',
  'fields.sighting_time.expression' = '#{date.past ''15'',''SECONDS''}'
);

select
  doctor,
  tumble_rowtime(sighting_time, interval '1' minute) as sighting_time,
  count(*) as sightings
from doctor_sightings
group by 
  tumble(sighting_time, interval '1' minute),
  doctor
;

-- 03 分析时间序列数据中的会话
CREATE TABLE server_logs ( 
    client_ip STRING,
    client_identity STRING, 
    userid STRING, 
    log_time TIMESTAMP(3),
    request_line STRING, 
    status_code STRING, 
    WATERMARK FOR log_time AS log_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'faker', 
  'rows-per-second' = '5',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.client_identity.expression' =  '-',
  'fields.userid.expression' =  '#{regexify ''(morsapaes|knauf|sjwiesman){1}''}',
  'fields.log_time.expression' =  '#{date.past ''5'',''SECONDS''}',
  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
);

select 
  userid,
  session_start(log_time, interval '10' second) as session_beg,
  session_end(log_time, interval '10' second) as session_end_se,
  session_rowtime(log_time, interval '10' second) as session_end_sr,
  count(request_line) as request_cnt
from server_logs
where status_code = '403'
group by 
  userid, 
  session(log_time, interval '10' second)
;

/*
sjwiesman 2022-12-09 07:33:26.682 2022-12-09 07:33:41.460                    3
sjwiesman 2022-12-09 07:33:54.481 2022-12-09 07:34:17.139                    5
如何理解, 26->41,  33:54->34:17; 
session_rowtime 如何理解该函数作用?
  指定 interval 间隔不活动即关闭, 依赖session_end 与 session_start 的差 > interval 
  上限的时间戳
  session_end 与 session_rowtime区别?
  后者可以用于后续的基于时间操作(连接,窗口), 后者会-1,好像用于水位线计算?
*/

-- 04 时间序列数据的滚动聚合
-- 这里有个过滤掉abs(平均值)超过四个标准差的温度测量值; 为什么要用平均值? 而不是测量值?
在上述场景中，如果平均值超过四个标准差，这意味着数据分布比较稀疏，且有较大的数据偏离平均值。例如，如果一个城市一年中气温的平均值超过四个标准差，这意味着这个城市气候波动范围较大，可能存在高温或低温的极端情况。
而如果测量值超过四个标准差，则表示某一个测量值与平均值的差异较大。例如，如果一个城市的某天气温超过四个标准差，这意味着这一天的气温与平均气温存在较大差异，


CREATE TEMPORARY TABLE temperature_measurements (
  measurement_time TIMESTAMP(3),
  city STRING,
  temperature FLOAT, 
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '15' SECONDS
)
WITH (
  'connector' = 'faker',
  'fields.measurement_time.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.temperature.expression' = '#{number.numberBetween ''0'',''50''}',
  'fields.city.expression' = '#{regexify ''(Chicago|Munich|Berlin|Portland|Hangzhou|Seatle|Beijing|New York){1}''}'
);


select 
  measurement_time,
  city,
  temperature,
  avg(cast(temperature as float)) over last_minute as avg_tt_m,
  min(temperature) over last_minute as min_tt_m,
  max(temperature) over last_minute as max_tt_m,
  stddev(cast(temperature as float)) over last_minute as stddev_tt_m
from temperature_measurements 
where city='Chicago'
window last_minute as (
  partition by city 
  order by measurement_time 
  range between interval '1' minute preceding and current row
)
;

/*
1.字段指定的 watermark 表示含义, 
数据是显示实际的event_time, 还是 event_time - watermark 值? 
  理解应该是用于窗口 join 操作时, 来依据的条件.
对于该例子, 统计1分钟窗口, 例如 current row, 统计1分钟, 还是 1m+15s(wm)?
*/


-- 05 continue top-N
每个 wizard top2 spell 

CREATE TABLE spells_cast (
    wizard STRING,
    spell  STRING
) WITH (
  'connector' = 'faker',
  'fields.wizard.expression' = '#{harry_potter.characters}',
  'fields.spell.expression' = '#{harry_potter.spells}'
);

-- fs 可以不加别名太友好了
select wizard, spell, time_cast, rn
from (
select 
  *,
  row_number() over(partition by wizard order by time_cast desc) as rn 
from (select wizard, spell, count(*) time_cast from spells_cast group by wizard, spell)
)
where rn <= 2
;
-- Sort on a non-time-attribute field is not supported; 
-- FS-IS-1 如何看到两个wizard排序在一起呢?


--06 duplicate
CREATE TABLE orders (
  id INT,
  order_time AS CURRENT_TIMESTAMP,
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECONDS
)
WITH (
  'connector' = 'datagen',
  'rows-per-second'='10',
  'fields.id.kind'='random',
  'fields.id.min'='1',
  'fields.id.max'='100'
);

-- Check for duplicates in the orders table
SELECT id AS order_id,
       COUNT(*) AS order_cnt
FROM orders o
GROUP BY id
HAVING COUNT(*) > 1;

-- use deduplication to keep only the latest record for each order_id
-- not use distinct? simple code? TODO use udf simple code?
select 
  order_id,
  order_time
from (
  select id as order_id, order_time
    ,row_number() over(partition by id order by order_time) as rn
  from orders
)
where rn=1;


--07 chain window
聚合两个不同粒度的时间序列数据
输出1分钟和5分钟窗口内的平均请求大小, 优化为单个source查询, 1分钟作为5分钟的输入

-- blackhole 这里测试 jobgraph 走单个source input
-- 这里是否注意操作放到一个statement操作?


--08 使用 match_recognize 模式检测(pattern recognition)
这个例子有点懵
先看看官方文档 https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/table/sql/queries/match_recognize/

将其服务订阅从高级层之一 ( type IN ('premium','platinum')) 降级到基本层的用户。
-- 用于查看消费降低的用户


measures 措施
premium 保险费
platinum 铂金
downgrade 使降级


CREATE TABLE subscriptions ( 
    id STRING,
    user_id INT,
    type STRING,
    start_date TIMESTAMP(3),
    end_date TIMESTAMP(3),
    payment_expiration TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.id.expression' = '#{Internet.uuid}', 
  'fields.user_id.expression' = '#{number.numberBetween ''1'',''50''}',
  'fields.type.expression'= '#{regexify ''(basic|premium|platinum){1}''}',
  'fields.start_date.expression' = '#{date.past ''30'',''DAYS''}',
  'fields.end_date.expression' = '#{date.future ''15'',''DAYS''}',
  'fields.payment_expiration.expression' = '#{date.future ''365'',''DAYS''}'
);


SELECT * 
FROM subscriptions
     MATCH_RECOGNIZE (PARTITION BY user_id 
                      ORDER BY proc_time
                      MEASURES
                        LAST(PREMIUM.type) AS premium_type,
                        AVG(TIMESTAMPDIFF(DAY,PREMIUM.start_date,PREMIUM.end_date)) AS premium_avg_duration,
                        BASIC.start_date AS downgrade_date
                      AFTER MATCH SKIP 
                      PAST LAST ROW
                      --Pattern: one or more 'premium' or 'platinum' subscription events (PREMIUM)
                      --followed by a 'basic' subscription event (BASIC) for the same `user_id`
                      PATTERN (PREMIUM+ BASIC)
                      DEFINE PREMIUM AS PREMIUM.type IN ('premium','platinum'),
                             BASIC AS BASIC.type = 'basic');


PREMIUM.type -> define premium 
BASIC.type -> define BASIC 
basic.start_date 怎么来的? 应该默认与wm绑定
-- 感觉sql的语法特性有点多了


select * 
from subscriptions
    -- 1.先定义分区和顺序, 输入
    match_recognize (partition by user_id 
                    order by proc_time 
                    -- 2.定义策略, 输出
                    measures 
                        last(premium.type) as premium_type,
                        avg(timestampdiff(day, premium.start_date, premium.end_date)) as premium_avg_duration, -- 持续天数
                        basic.start_date as downgrade_date -- 降级日期
                    after match skip  -- 下次匹配开始位置
                    past last row -- [skip] 省略, 赛后策略, 匹配最后一行后,从下行恢复匹配
                    -- 3.模式定义, 策略变量与事件匹配
                    pattern (premium+ basic) -- 多个事件要用+表示, 支持正则
                    define premium as premium.type in ('premium', 'platinum'),
                        basic as basic.type='basic');

-- 官方案例的股票价格段 symbol start_ts, bottom_ts, up_ts



--09 使用变更数据捕获(CDC) 和 Debezium 维护物化视图
debezium 是 cdc 底层引擎? 该示例与flink cdc 从binlog获取数据的区别?
    CDC Connectors for Apache Flink®集成了 Debezium 作为捕获数据更改的引擎。

-- 参考这个, 写一个mysql cdc从binlog取数的demo
-- checkpoint every 3000 milliseconds                       
Flink SQL> SET 'execution.checkpointing.interval' = '3s';   

-- register a MySQL table 'orders' in Flink SQL
Flink SQL> CREATE TABLE table_process (
     source_table string,
     operate_type string,
     sink_type STRING,
     sink_table string,
     sink_columns string,
     sink_pk string,
     sink_extend string,
     primary key(source_table, operate_type) not ENFORCED
     ) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'node1',
     'port' = '3306',
     'username' = 'root',
     'password' = 'root',
     'database-name' = 'gmall-210325-realtime',
     'table-name' = 'table_process'
     );
  
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: The primary key is necessary when enable 'Key: 'scan.incremental.snapshot.enabled' , default: true (fallback keys: [])' to 'true'

Non-query expression encountered in illegal context



-- read snapshot and binlogs from orders table
Flink SQL> SELECT * FROM table_process;



-- 10. 跳跃时间的窗口
-- 每30秒, 显示1分钟每种货币的出价平均值
hop函数, 与 tumble (TODO 该case待考虑)区别在于, 可以及时 "跳跃", 间隔与窗口长度关系;

SELECT
    window_start, window_end, currency_code
    ,round(avg(bid_price), 2) as MovingAverageBidPrice
from table(
    HOP(TABLE bids, DESCRIPTOR(transaction_time), interval '30' seconds, interval '1' minute)
)
group by window_start, window_end, currency_code; 


-- 11 window Top-N
每个滚动(多久统计一次? 每五分钟?)5分钟窗口中销售额最高的前三名供应商
-- 这里使用 top-n 功能, 相当于简化代码, 仅发出最终结果, 不用手动排序;
-- TODO 有点没理解
    -- 不会输出中间数据, 不用手动更新最新TOP3显示结果;(这个对比之前的sql没有理解)

-- https://github.com/ververica/flink-sql-cookbook/blob/main/aggregations-and-analytics/11_window_top_n/11_window_top_n.md


select * from (
    select *, row_number() over(partition by window_start, window_end order by price desc) rn
    from (
        select window_start, window_end,supplier, sum(price) price, count(*) cnt
        from table(
            tumble(table orders, descriptor(bidtime), interval '10' minutes)
        )
        group by window_start, window_end,supplier
    )    
)
where rn<=3
;

tumble 翻滚

-- 12 不进行自连接的情况检索上一行值
lag 函数


WITH current_and_previous as (
    select 
        stock_name,
        log_time, 
        stock_value, 
        lag(stock_value, 1) over (partition by stock_name order by log_time) previous_value 
    from fake_stocks
)
select *, 
    case 
        when stock_value > previous_value then '▲'
        when stock_value < previous_value then '▼'
        else '=' 
    end as trend 
from current_and_previous;

-- 这个上三角很有意思;


