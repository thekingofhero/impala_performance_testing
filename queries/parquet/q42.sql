select
  dt.d_year,
  item_parquet.i_category_id,
  item_parquet.i_category,
  sum(ss_ext_sales_price)
from
  date_dim_parquet dt,
  store_sales_parquet,
  item_parquet
where
  dt.d_date_sk = store_sales_parquet.ss_sold_date_sk
  and store_sales_parquet.ss_item_sk = item_parquet.i_item_sk
  and item_parquet.i_manager_id = 1
  and dt.d_moy = 12
  and dt.d_year = 1998
  and ss_sold_date_sk between 2451149 and 2451179  -- partition key filter
group by
  dt.d_year,
  item_parquet.i_category_id,
  item_parquet.i_category
order by
  sum(ss_ext_sales_price) desc,
  dt.d_year,
  item_parquet.i_category_id,
  item_parquet.i_category
limit 100;

profile;