select
  dt.d_year,
  item_text.i_brand_id brand_id,
  item_text.i_brand brand,
  sum(ss_ext_sales_price) ext_price
from
  date_dim_text dt,
  store_sales_text,
  item_text
where
  dt.d_date_sk = store_sales_text.ss_sold_date_sk
  and store_sales_text.ss_item_sk = item_text.i_item_sk
  and item_text.i_manager_id = 1
  and dt.d_moy = 12
  and dt.d_year = 1998
  and ss_sold_date_sk between 2451149 and 2451179 -- added for partition pruning
group by
  dt.d_year,
  item_text.i_brand,
  item_text.i_brand_id
order by
  dt.d_year,
  ext_price desc,
  brand_id
limit 100;

profile;
