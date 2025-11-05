with orders as ( 
    select
    orders_id,
    date_date,
    sum(revenue) as revenue,
    sum(quantity) as quantity,
    sum(purchase_cost) as purchase_cost,
    sum(margin) as margin
    from {{ ref('int_sales_margin')}}
    group by date_date, orders_id
),

ship as (
    select
    orders_id,
    shipping_fee,
    logcost,
    cast(ship_cost as numeric) as ship_cost
    from {{ ref('stg_raw__ship')}}
)

select 
o.orders_id,
o.date_date,
ROUND((o.margin + s.shipping_fee - s.logcost - s.ship_cost),2) as operational_margin,
o.quantity,
o.revenue,
o.purchase_cost,
o.margin,
s.shipping_fee,
s.logcost,
s.ship_cost
from orders o 
left join ship s 
using(orders_id)
order by o.date_date, o.orders_id