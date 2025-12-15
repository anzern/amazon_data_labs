with source as (

    select
    id              ,
    gender          ,
	masterCategory  ,
    subCategory     ,
    articleType     ,
    baseColor       ,
    season          ,
    year            ,
    usage           ,
    productDisplayName 

    from {{ source('source', 'src__product') }}

)


select *
from source
