select * from {{ ref('stg_adwords') }}
union all
select * from {{ ref('stg_bing') }}
union all
select * from {{ ref('stg_criteo') }}
union all
select * from {{ ref('stg_facebook') }}