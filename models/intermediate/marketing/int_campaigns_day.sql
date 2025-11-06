select
  date_date,
  sum(ads_cost)                    as total_ads_cost,
  sum(impression)                  as total_impressions,
  sum(click)                       as total_clicks,
  sum(if(lower(paid_source) = 'adwords',  ads_cost, 0)) as adwords_ads_cost,
  sum(if(lower(paid_source) = 'bing',     ads_cost, 0)) as bing_ads_cost,
  sum(if(lower(paid_source) = 'criteo',   ads_cost, 0)) as criteo_ads_cost,
  sum(if(lower(paid_source) = 'facebook', ads_cost, 0)) as facebook_ads_cost,
  countif(lower(paid_source) = 'adwords')  as nb_adwords,
  countif(lower(paid_source) = 'bing')     as nb_bing,
  countif(lower(paid_source) = 'criteo')   as nb_criteo,
  countif(lower(paid_source) = 'facebook') as nb_facebook
from {{ ref('int_campaigns') }}
group by 1
order by date_date desc, total_ads_cost desc
