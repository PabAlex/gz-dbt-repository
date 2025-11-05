with 

source as (

    select * from {{ source('raw_circle', 'cc_parcel') }}

),

renamed as (

    select
        Parcel_id as parcel_id,
        Parcel_tracking as parcel_tracking,
        Transporter as transporter,
        Priority as priority,
        SAFE.PARSE_DATE('%B %e, %Y', Date_purCHase) as date_purchase,
        SAFE.PARSE_DATE('%B %e, %Y', Date_sHIpping) as date_shipping,
        SAFE.PARSE_DATE('%B %e, %Y', DATE_delivery) as date_delivery,
        SAFE.PARSE_DATE('%B %e, %Y', DaTeCANcelled) as datecancelled

    from source

)

select * from renamed