with p as (
  select * from {{ ref('stg_cc_parcel') }}
),
pp as (
  select * from {{ ref('stg_cc_parcel_products') }}
),

enriched as (
  select
    p.parcel_id,
    p.parcel_tracking,
    p.transporter,
    p.priority,
    p.date_purchase,
    p.date_shipping,
    p.date_delivery,
    p.date_cancelled,

    extract(month from p.date_purchase) as month_purchase,
    case
      when p.date_cancelled is not null then 'cancelled'
      when p.date_delivery is not null then 'delivered'
      when p.date_shipping is not null then 'shipped'
      when p.date_purchase is not null then 'ordered'
      else 'unknown'
    end as status,

    safe_cast(date_diff(p.date_shipping, p.date_purchase, day) as int64) as expedition_time,
    safe_cast(date_diff(p.date_delivery, p.date_shipping, day) as int64) as transport_time,
    safe_cast(date_diff(p.date_delivery, p.date_purchase, day) as int64) as delivery_time,
    greatest(safe_cast(date_diff(p.date_delivery, p.date_purchase, day) as int64), 0) as delay,

    sum(pp.quantity) as qty,
    count(distinct pp.model_name) as nb_model
  from p
  left join pp on p.parcel_id = pp.parcel_id
  group by
    p.parcel_id,
    p.parcel_tracking,
    p.transporter,
    p.priority,
    p.date_purchase,
    p.date_shipping,
    p.date_delivery,
    p.date_cancelled
)

select * from enriched
