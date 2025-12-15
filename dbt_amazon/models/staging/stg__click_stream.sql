with source as (

    select
    session_id      ,
    event_name      ,
    event_time      ,
    event_id        ,
    traffic_source  ,
    event_metadata 
    from {{ source('source', 'src__click_stream') }}

)


select *
from source
