with daily as (
    select *
    from {{ ref ('finance_days')}}
    left join {{ref('int_campaigns_day')}}
    using(date_date)
)

select
date_date,
round(operational_margin - total_ads_cost,2) as ads_margin,
average_basket,
operational_margin,
total_ads_cost as ads_cost,
total_impressions as ads_impressions,
total_clicks as ads_clicks,
total_quantity as quantity,
total_revenue as revenue,
total_purchase_cost as purchase_cost,
total_margin as margin,
total_shipping_fees as shipping_fee,
total_log_costs as log_cost,
total_ship_cost as ship_cost

from daily