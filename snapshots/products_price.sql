{% snapshot products_snapshot %}

{{
  config(
    target_schema = 'dbt_snapshots',
    unique_key    = 'products_id',
    strategy      = 'check',
    check_cols    = ['purchase_price']
  )
}}

select
  products_id,
  purchase_price,
  category
from {{ ref('stg_raw__product') }}

{% endsnapshot %}
