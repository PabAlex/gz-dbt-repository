{% snapshot products_snapshot %}

{{
    config(
      target_schema='dbt_snapshots', --corresponding to the new database in BigQuery
      unique_key='products_id', --unique key of the table to track
      strategy='check', --other strategy
      check_cols='all' --track evolution on all columns
    )
}}

select * from {{ ref("stg_raw__product") }}

{% endsnapshot %}