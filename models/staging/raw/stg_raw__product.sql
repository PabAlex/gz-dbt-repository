with 

source as (

    select * from {{ source('raw', 'product') }}

),

renamed as (

    select
        products_id,
        CAST(purchse_price as FLOAT64) AS purchase_price

    from source
    WHERE products_id IS NOT NULL

)

select * from renamed