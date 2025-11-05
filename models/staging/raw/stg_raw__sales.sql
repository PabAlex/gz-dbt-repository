with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        cast(date_date as timestamp) as date_date,
        orders_id,
        pdt_id AS products_id,
        revenue,
        quantity

    from source

)

select * from renamed