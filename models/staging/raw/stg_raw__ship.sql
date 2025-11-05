with

source as (
    select * from {{ source('raw', 'ship') }}
),

renamed as (
    select
        orders_id,
        shipping_fee,                   
        cast(ship_cost as numeric) as ship_cost,
        logcost
    from source
)

select * from renamed
