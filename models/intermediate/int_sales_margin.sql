with
sales as (
  select 
  date_date,
  orders_id,
  products_id,
  quantity,
  revenue
   from {{ ref('stg_raw__sales') }}
),
product as (
  select 
  products_id,
  CAST(purchase_price as FLOAT64) AS purchase_price
  from {{ ref('stg_raw__product') }}
)

select 
s.date_date,
s.orders_id,
s.products_id,
s.quantity,
s.revenue,
p.purchase_price,
ROUND((s.quantity * p.purchase_price),2) AS purchase_cost,
ROUND((s.revenue - (s.quantity * p.purchase_price)),2) AS margin,
{{ margin_percent(revenue, purchase_cost) }} as margin_percent
from sales s
left join product p
  using (products_id)
