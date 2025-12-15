with source as (

    select
    created_at  ,
    customer_id ,
	booking_id  ,
    session_id  ,
    product_metadata ,
    payment_method ,
    payment_status ,
    promo_amount ,
    promo_code ,
    shipment_fee ,
    shipment_date_limit ,
    shipment_location_lat ,
    shipment_location_long ,
	total_amount 


    from {{ source('source', 'src__transactions') }}

)


select *
from source
