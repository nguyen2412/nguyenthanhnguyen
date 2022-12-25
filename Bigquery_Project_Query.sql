-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Thụt dòng cho từng đoạn, từng phần để dễ nhìn hơn
--k nên đặt tên CTE là cte hoặc ABC,nên đặt tên viết tắt, mà nhìn vào mình có thể hiểu đc CTE đó đang lấy data gì

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
--a sẽ chỉnh lại bài, thụt dòng theo 1 nguyên tắc, e coi coi dễ nhìn hơn k

SELECT 
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date )) AS month,
    count(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0101'and '0331'
group by month  --group by 1
order by month; --order by 1

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
    order by 1,2 desc)
,
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

--bài này yêu cầu mình lấy những sản phẩm mà được mua bởi nhóm khách hàng (A), là nhóm mà đã từng mua sp Youtube
--step 1 mình sẽ lấy ds nhóm khách hàng (A), đã từng mua sp Youtube
--step 2 mình lấy những sp đc mua, loại sp youtube ra
--subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and hits.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc

--CTE:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with x as (
    SELECT 
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

--thường vs những đề bài phức tạp, a sẽ dùng CTE, để dễ kiểm soát câu query hơn
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month


Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data

                                      
                                              ---VERY GOOD---

