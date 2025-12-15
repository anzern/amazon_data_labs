{% set materialization = 'table' %}   

-- 'table' | 'incremental'

{{ 
config( 
    materialized = materialization,
    unique_key   = 'event_id' if materialization == 'incremental' else none
) }}



{% set model_name = ref('stg__product') %}

{% set columns = [
    ('id'               ,   'integer'),
    ('gender'           ,   'text'),
    ('masterCategory'   ,   'text'),
    ('subCategory'      ,   'text'),
    ('articleType'      ,   'text'),
    ('baseColor'        ,   'text'),
    ('season'           ,   'text'),
    ('year'             ,   'integer'),
    ('usage'            ,   'text'),
    ('productDisplayName',  'text')
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
