with source as (

    select
    customer_id ,
    first_name  ,
	last_name   ,
    username    ,
    email       ,
    gender      ,
    birth_date  ,
    device_type ,
    device_version ,
    device_id ,
    home_location_lat ,
    home_location_long ,
    home_location , 
    home_country ,
    first_join_date 

    from {{ source('source', 'src__customer') }}

)


select *
from source
