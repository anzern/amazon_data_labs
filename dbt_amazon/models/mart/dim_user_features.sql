{% set materialization = 'table' %}

with clicks as (

    select * , 
    EXTRACT(EPOCH FROM (event_time - LAG(event_time) OVER (ORDER BY event_time))) / 60 AS duration_interval



    from {{ ref('int_click_stream') }}

)

, customers as (

    select *  
    from {{ ref('int_customer') }}

)

, product as (

    select *  
    from {{ ref('int_product') }}
)

, transactions as (

    select *  
    from {{ ref('int_transactions') }}
)


, customer_features as (
select 
u.customer_id, 
u.gender,
u.device_type, 
u.birth_date, 
home_location, 
EXTRACT(YEAR FROM AGE(first_join_date, birth_date)) as age
    from customers u
)


, transaction_features as (
select 
    u.customer_id, 
    count(t.booking_id) as total_transactions,
    date(max(t.created_at)) as last_transaction_date,
    count(case when promo_code is not null then 1 end)/count(*) as promo_used_count,
    count(case when payment_method = 'Credit Card' then 1 end)/count(*) as credit_card_usage,
    PERCENTILE_CONT(0.5) within group (order by EXTRACT(EPOCH FROM (t.shipment_date_limit - t.created_at))) / 3600 AS ship_hours_diff,
    PERCENTILE_CONT(0.5) within group (order by total_amount) AS median_trx_amount,
    count(case 
    when EXTRACT(day FROM t.created_at) >=24 then 1 
    when EXTRACT(day FROM t.created_at) = EXTRACT(month FROM t.created_at) then 1
     end)/count(*) as promo_buying_period_ratio,
    count(case when t.quantity > 1 then 1 end)/count(*) as multi_quantity_ratio
    from customers u
    inner join transactions t on u.customer_id = t.customer_id
group by 1
)


, product_features as (
select 
    u.customer_id, 
    sum(case when p.gender in ('Men', 'Boys') then 1 else 0 end)/count(*) as male_product_ratio,
    sum(case when p.gender in ('Woman', 'Girls') then 1 else 0 end)/count(*) as female_product_ratio,
    sum(case when p.gender in ('Unisex') or p.gender is null then 1 else 0 end)/count(*) as unisex_product_ratio,
    sum(case when p.masterCategory in ('Free Items') then 1 else 0 end)/count(*) as free_items_ratio,
    sum(case when p.masterCategory in ('Accessories') then 1 else 0 end)/count(*) as accessories_product_ratio,
    sum(case when p.masterCategory in ('Footwear') then 1 else 0 end)/count(*) as footwear_product_ratio,
    sum(case when p.masterCategory in ('Sporting Goods') then 1 else 0 end)/count(*) as sporting_goods_product_ratio,
    sum(case when p.masterCategory in ('Apparel') then 1 else 0 end)/count(*) as apparel_product_ratio,
    sum(case when p.masterCategory in ('Home') then 1 else 0 end)/count(*) as home_product_ratio,
    sum(case when p.masterCategory in ('Personal Care') then 1 else 0 end)/count(*) as personal_care_product_ratio        
    from customers u
    inner join transactions t on u.customer_id = t.customer_id
    inner join product p on t.product_id = p.id
group by 1
)

, click_features_b as (
select 
    u.customer_id, 
    t.booking_id,
    t.created_at,
    count(event_id) as total_events,
    EXTRACT(EPOCH FROM (t.created_at - MIN(c.event_time) )) AS first_event_booking_to_sec,
    count(case when duration_interval > 300 then 1  end)/(count(event_id) - 1) as hibrnate_events,
    count(case when event_name = 'SEARCH' then 1  end)/(count(event_id)) as search_events,
    count(case when event_name = 'PROMO_PAGE' then 1  end)/(count(event_id)) as promo_page_events,
    count(case when event_name = 'SCROLL' then 1  end)/(count(event_id)) as scroll_events
    
    from customers u
    inner join transactions t on u.customer_id = t.customer_id
    inner join clicks c on t.session_id = c.session_id
group by 1,2,3
)

, click_features_final as (
select 
    u.customer_id, 
    avg(total_events) as avg_events_per_booking,
    avg(first_event_booking_to_sec) as avg_first_event_booking_to_sec,
    avg(hibrnate_events) as avg_hibrnate_events,
    avg(search_events) as avg_search_events,
    avg(promo_page_events) as avg_promo_page_events,
    avg(scroll_events) as avg_scroll_events
    
    from click_features_b u
group by 1
)


select 
    cf.customer_id,
    cf.device_type, 
    cf.home_location, 
    cf.age,
    cf.gender,
    date('2022-08-01') - tf.last_transaction_date as recency_days,
    tf.total_transactions,
    tf.promo_used_count,
    tf.credit_card_usage,
    tf.ship_hours_diff,
    tf.median_trx_amount,
    tf.promo_buying_period_ratio,
    tf.multi_quantity_ratio,
    pf.male_product_ratio,
    pf.female_product_ratio,
    pf.unisex_product_ratio,
    pf.free_items_ratio,
    pf.accessories_product_ratio,
    pf.footwear_product_ratio,
    pf.sporting_goods_product_ratio,
    pf.apparel_product_ratio,
    pf.home_product_ratio,
    pf.personal_care_product_ratio,
    cf2.avg_events_per_booking,
    cf2.avg_first_event_booking_to_sec,
    cf2.avg_hibrnate_events,
    cf2.avg_search_events,
    cf2.avg_promo_page_events,
    cf2.avg_scroll_events


FROM customer_features cf
left join transaction_features tf on cf.customer_id = tf.customer_id
left join product_features pf on cf.customer_id = pf.customer_id
left join click_features_final cf2 on cf.customer_id = cf2.customer_id
