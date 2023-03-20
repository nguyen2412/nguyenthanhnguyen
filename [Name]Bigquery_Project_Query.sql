-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
count(totals.visits) as visits,
sum(totals.pageviews) as pageviews,
sum(totals.transactions) as transactions,
sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0101'and '0331'
group by month
order by month;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT distinct trafficSource.source as source,
Sum(totals.visits) as total_visits,
Sum(totals.bounces) as total_no_of_bounces,
Sum(totals.bounces)/Sum(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
group by source
order by total_visits desc;
-- Query 3: Revenue by traffic source by week, by month in June 2017

with bcd as 
(SELECT
  format_date("%Y%m",parse_date("%Y%m%d",date)) as m,
  sum(totals.transactionRevenue) ,
trafficSource.source,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
where totals.transactionRevenue is  not null
group by m,trafficSource.source
order by 1,2 desc),
 abc as 
(SELECT
  format_date("%Y%W",parse_date("%Y%m%d",date)) as w,
sum(totals.transactionRevenue),
trafficSource.source
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where totals.transactionRevenue is  not null
group by w,trafficSource.source
order by 1,2 desc)
select * from bcd 
union all 
select * from abc
order by 2 desc;
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 
Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0601' and '0731'
and totals.transactions is null
group by month
order by month;
SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0601' and '0731'
and totals.transactions is not null
group by month
order by month;



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE 
_table_suffix between '0701'and'0731' and totals.transactions >=1
group by month
order by month;
-- Query 06: Average amount of money spent per session
#standardSQL

SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
sum(totals.transactions)/
count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where 
_table_suffix between '0701' and '0731'
 and totals.transactions is not null
 group by month;


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with x as (SELECT 
FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
count(case when hits.eCommerceAction.action_type = '2' then 1 end) as num_product_view,
count(case when hits.eCommerceAction.action_type = '3' then 1 end) as 
num_addtocart,
count(case when hits.eCommerceAction.action_type = '6' then 1 end) as num_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` ,
unnest(hits) as hits,
unnest(hits.product) as product
where _table_suffix between '0101' and '0331'
group by month
order by month)
select month,num_product_view,
num_addtocart,num_purchase,
(num_purchase/num_product_view*100) as add_to_cart_rate,
(num_addtocart/num_product_view*100) as purchase_rate
 from x
 
Unnest sử dụng khi mình làm việc với các array (hay dạng struct). Khi cậu không unnest thì nó sẽ báo lỗi.
Ví dụ như ở câu 7:
FROM
  bigquery-public-data.google_analytics_sample.ga_sessions_201707*,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where product.v2ProductName="YouTube Men's Vintage Henley"

Mọi người chú ý những dấu , dấu "" nhé
Nếu mình ko có bước
UNNEST (hits) hits,
UNNEST (hits.product) product

mà mình dùng hits.product.v2ProductName thì nó sẽ báo lỗi do bên trong nó là 1 cái array.
Bản chất của unnest nó giống như cross join ấy. Nó giúp mình tách từng thành phần bên trong array đó ra.
Ví dụ như data mình có 2 cột, giá trị là:

col1      col2
X       [1,2,3,4]

Sau khi unnest(col2) thì output sẽ như sau:
X   1
X   2
X   3
X   4"