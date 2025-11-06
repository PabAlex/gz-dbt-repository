-- Agrégat par mois
with monthly as (
  select
    date_trunc(date_date, month) as datemonth,
    sum(total_transactions) as total_transactions,
    sum(quantity) as quantity,
    sum(revenue) as revenue,
    sum(purchase_cost) as purchase_cost,
    sum(margin) as margin,
    sum(shipping_fee) as shipping_fee,
    sum(log_cost) as log_cost,
    sum(ship_cost) as ship_cost,
    sum(operational_margin) as operational_margin,
    sum(ads_cost) as ads_cost,
    sum(ads_impressions) as ads_impression,
    sum(ads_clicks) as ads_clicks
  from {{ ref('finance_campaigns_day') }}
  group by 1
)

select
  datemonth,
  round(operational_margin - ads_cost, 2) as ads_margin,
  round(revenue / nullif(total_transactions,0), 2 ) as average_basket,
  operational_margin,
  ads_cost as ads_cost,
  ads_impression as ads_impression,
  ads_clicks as ads_clicks,
  quantity,
  round(revenue, 2 ) as revenue,
  round(purchase_cost, 2 ) as purchase_cost,
  round(margin, 2 ) as margin,
  round(shipping_fee, 2 ) as shipping_fee,
  round(log_cost, 2 ) as log_cost,
  round(ship_cost, 2 ) as ship_cost
from monthly
order by datemonth desc
