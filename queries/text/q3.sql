select
  dt.d_year,
  item_text.i_brand_id brand_id,
  item_text.i_brand brand,
  sum(ss_ext_sales_price) sum_agg
from
  date_dim_text dt,
  store_sales_text,
  item_text
where
  dt.d_date_sk = store_sales_text.ss_sold_date_sk
  and store_sales_text.ss_item_sk = item_text.i_item_sk
  and item_text.i_manufact_id = 436
  and dt.d_moy = 12
  -- partition key filters
  and (ss_sold_date_sk between 2451149 and 2451179
    or ss_sold_date_sk between 2451514 and 2451544
    or ss_sold_date_sk between 2451880 and 2451910
    or ss_sold_date_sk between 2452245 and 2452275
    or ss_sold_date_sk between 2452610 and 2452640)
group by
  dt.d_year,
  item_text.i_brand,
  item_text.i_brand_id
order by
  dt.d_year,
  sum_agg desc,
  brand_id
limit 100;

profile;
