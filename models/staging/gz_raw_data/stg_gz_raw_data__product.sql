with 

source as (

    select * from {{ source('gz_raw_data', 'product') }}

),

renamed as (

    select
        products_id,
        purchse_price as purchase_price

    from source

)

select * from renamed
