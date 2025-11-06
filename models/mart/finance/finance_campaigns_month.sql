-- Agrégat par mois
with montly as (
  select
    date_trunc(date_date, month) as datemonth,
    sum(total_transactions) as total_transactions,
    sum(total_quantity) as quantity,
    sum(total_revenue) as revenue,
    sum(total_purchase_cost) as purchase_cost,
    sum(total_margin) as margin,
    sum(total_shipping_fees) as shipping_fee,
    sum(total_log_costs) as log_cost,
    sum(total_ship_cost) as ship_cost,
    sum(operational_margin) as operational_margin,
    sum(total_ads_cost) as ads_cost,
    sum(total_impressions) as ads_impression,
    sum(total_clicks) as ads_clicks
  from {{ ref('finance_campaigns_days') }}
  group by 1
),

select
  datemonth,
  round(operational_margin - ads_cost, 2) as ads_margin,
  round(revenue / nullif(total_transactions,0), 2 ) as average_basket,
  operational_margin,
  ads_cost as ads_cost,
  ads_impression as ads_impression,
  ads_clicks as ads_clicks,
  quantity,
  revenue,
  purchase_cost,
  margin,
  shipping_fee,
  log_cost,
  ship_cost
from monthly
order by datemonth desc
