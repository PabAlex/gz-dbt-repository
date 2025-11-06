with orders_op as (
  select
    date_date,
    orders_id,
    revenue,
    quantity,
    purchase_cost,
    shipping_fee,
    logcost as log_cost,
    ship_cost,
    margin,
    operational_margin
  from {{ ref('int_orders_operational') }}
),

daily as (
  select
    date(date_date) as date_date,
    count(distinct orders_id) as total_transactions,
    ROUND(sum(revenue),2) as total_revenue,
    ROUND(safe_divide(sum(revenue), nullif(count(distinct orders_id),0)),2) as average_basket,
    ROUND(sum(operational_margin),2) as operational_margin,
    ROUND(sum(purchase_cost),2) as total_purchase_cost,
    ROUND(sum(margin),2) as total_margin,
    ROUND(sum(shipping_fee),2) as total_shipping_fees,
    ROUND(sum(log_cost),2) as total_log_costs,
    ROUND(sum(quantity),2) as total_quantity,
    ROUND(sum(ship_cost),2) as total_ship_cost
  from orders_op
  group by date_date
)

select *
from daily
order by date_date
