select
  dt.d_year,
  item_parquet.i_brand_id brand_id,
  item_parquet.i_brand brand,
  sum(ss_ext_sales_price) sum_agg
from
  date_dim_parquet dt,
  store_sales_parquet,
  item_parquet
where
  dt.d_date_sk = store_sales_parquet.ss_sold_date_sk
  and store_sales_parquet.ss_item_sk = item_parquet.i_item_sk
  and item_parquet.i_manufact_id = 436
  and dt.d_moy = 12
  -- partition key filters
  and (ss_sold_date_sk between 2451149 and 2451179
    or ss_sold_date_sk between 2451514 and 2451544
    or ss_sold_date_sk between 2451880 and 2451910
    or ss_sold_date_sk between 2452245 and 2452275
    or ss_sold_date_sk between 2452610 and 2452640)
group by
  dt.d_year,
  item_parquet.i_brand,
  item_parquet.i_brand_id
order by
  dt.d_year,
  sum_agg desc,
  brand_id
limit 100;

profile;
