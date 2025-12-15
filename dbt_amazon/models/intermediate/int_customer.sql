{% set materialization = 'table' %}   

-- 'table' | 'incremental'

{{ 
config( 
    materialized = materialization,
    unique_key   = 'event_id' if materialization == 'incremental' else none
) }}



{% set model_name = ref('stg__customer') %}

{% set columns = [
    ('customer_id'          ,   'integer'),
    ('first_name'           ,   'text'),
    ('last_name'            ,   'text'),
    ('username'             ,   'uuid'),
    ('email'                ,   'text'),
    ('gender'               ,   'text'),
    ('birth_date'           ,   'date'),
    ('device_type'          ,   'text'),
    ('device_version'       ,   'uuid'),
    ('device_id'            ,   'text'),
    ('home_location_lat'    ,   'decimal'),
    ('home_location_long'   ,   'decimal'),
    ('home_location'        ,   'text'),
    ('home_country'         ,   'text'),
    ('first_join_date'      ,   'date')
] %}

with base as (

    select
        *
    from {{ model_name }}

    {% if is_incremental() %}
      where event_time > (select max(event_time) from {{ this }})
    {% endif %}

),

flattened as (

    select
        *
    from base
)

select
    {{ select_with_cast(columns) }}
from flattened
