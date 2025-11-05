with source as (
  select * from {{ source('raw_circle', 'cc_parcel_product') }}
),

renamed as (
  select
    ParCEL_id   as parcel_id,
    Model_mAME  as model_name,
    cast(QUANTITY as int64) as quantity
  from source
)

select * from renamed
