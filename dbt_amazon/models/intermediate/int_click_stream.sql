{% set materialization = 'table' %}   

-- 'table' | 'incremental'

{{ 
config( 
    materialized = materialization,
    unique_key   = 'event_id' if materialization == 'incremental' else none
) }}



{% set model_name = ref('stg__click_stream') %}


{% set columns = [
    ('session_id',       'text'),
    ('event_id',         'text'),
    ('event_name',       'text'),
    ('event_time',       'timestamp'),
    ('traffic_source',   'text'),
    ('product_id',       'text'),
    ('quantity',       'integer'),
    ('item_price',       'integer'),
    ('payment_status',   'text'),
    ('search_keywords',  'text'),
    ('promo_code',       'text'),
    ('promo_amount',     'integer')
] %}

with base as (

    select
        session_id,
        event_id,
        event_name,
        event_time::timestamp as event_time,
        traffic_source,
        {{ safe_json('event_metadata') }} event_metadata
    from {{ model_name }}

    {% if is_incremental() %}
      where event_time > (select max(event_time) from {{ this }})
    {% endif %}

),

flattened as (

    select
        session_id,
        event_id,
        event_name,
        event_time,
        traffic_source,
        (event_metadata ->> 'product_id')::integer as product_id,
        (event_metadata ->> 'quantity')::integer as quantity,
        (event_metadata ->> 'item_price')::integer as item_price,
        (event_metadata ->> 'payment_status')::text as payment_status,
        (event_metadata ->> 'search_keywords')::text  as search_keywords,
        (event_metadata ->> 'promo_code')::text as promo_code,
        (event_metadata ->> 'promo_amount')::integer as promo_amount
    from base
)

select
    {{ select_with_cast(columns) }}
from flattened
