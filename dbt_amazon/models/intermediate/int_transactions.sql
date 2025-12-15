{% set materialization = 'table' %}   

-- 'table' | 'incremental'

{{ 
config( 
    materialized = materialization,
    unique_key   = 'event_id' if materialization == 'incremental' else none
) }}



{% set model_name = ref('stg__transactions') %}


{% set columns = [
    ('created_at'               ,   'timestamp'),
    ('customer_id'              ,   'integer'),
    ('booking_id'               ,   'uuid'),
    ('session_id'               ,   'uuid'),
    ('product_id'               ,   'integer'),
    ('quantity'                 ,   'integer'),
    ('item_price'               ,   'integer'),
    ('payment_method'           ,   'text'),
    ('payment_status'           ,   'text'),
    ('promo_amount'             ,   'integer'),
    ('promo_code'               ,   'text'),
    ('shipment_fee'             ,   'integer'),
    ('shipment_date_limit'      ,  'timestamp'),
    ('shipment_location_lat'    ,  'decimal'),
    ('shipment_location_long'   ,  'decimal'),
    ('total_amount'             ,  'integer')
] %}

with base as (

    select
            created_at  ,
            customer_id ,
	        booking_id  ,
            session_id  ,
            {{ safe_json_array('product_metadata') }} product_metadata ,
            payment_method ,
            payment_status ,
            promo_amount ,
            promo_code ,
            shipment_fee ,
            shipment_date_limit ,
            shipment_location_lat ,
            shipment_location_long ,
	        total_amount 

    from {{ model_name }}

    {% if is_incremental() %}
      where event_time > (select max(event_time) from {{ this }})
    {% endif %}

),

flattened as (

    select
        created_at  ,
        customer_id ,
        booking_id  ,
        session_id  ,
        (product_metadata->0 ->> 'product_id')::integer as product_id,
        (product_metadata->0 ->> 'quantity')::integer as quantity,
        (product_metadata->0 ->> 'item_price')::integer as item_price,
        payment_method ,
        payment_status ,
        promo_amount ,
        promo_code ,
        shipment_fee ,
        shipment_date_limit ,
        shipment_location_lat ,
        shipment_location_long ,
        total_amount 
    from base
)

select
    {{ select_with_cast(columns) }}
from flattened
