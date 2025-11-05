with source as (
    select
    orders_id,
    date_date,
    revenue,
    quantity,
    cast(purchase_cost as float64) as purchase_cost,
    margin
    from {{ ref('int_sales_margin') }}
),

orders_agg as (
    select
    orders_id,
    date_date,
    ROUND(sum(revenue),2) as revenue,
    ROUND(sum(quantity),0) as quantity,
    ROUND(sum(purchase_cost),2) as purchase_cost,
    ROUND(sum(margin),2) as margin
    from source
    group by date_date, orders_id
)

select * 
from orders_agg
order by date_date, orders_id DESC