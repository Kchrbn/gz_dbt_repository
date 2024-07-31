
with

sales_data as (
    select
        date_date,
        orders_id,
        products_id,
        revenue,
        cast(quantity as int64) as quantity  -- Cast to numeric type
    from {{ ref('stg_raw_data__sales') }}
),

product_data as (
    select
        products_id,
        cast(purchase_price as numeric) as purchase_price  -- Cast to numeric type
    from {{ref('stg_raw_data__product')}}
),

ship_data as (
    select
        orders_id,
        shipping_fee,
        logcost,
        ship_cost
    from {{ ref('stg_raw_data__ship') }}
),

joined_data as (
    select
        sd.date_date,
        sd.orders_id,
        sd.products_id,
        sd.revenue,
        sd.quantity,
        pd.purchase_price,
        sp.shipping_fee,
        sp.logcost,
        sp.ship_cost,
        (sd.quantity * pd.purchase_price) as purchase_cost,
        (sd.revenue - (sd.quantity * pd.purchase_price)) as margin
    from sales_data sd
    join product_data pd on sd.products_id = pd.products_id
    join ship_data sp on sd.orders_id = sp.orders_id
)

select * from joined_data

