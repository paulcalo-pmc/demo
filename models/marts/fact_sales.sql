{{ config(
    materialized='table',
    tags=['mart', 'fact']
) }}

-- Keep only January 2000 data so the fact model runs fast
with sales as (
    select *
    from {{ ref('stg_tpcds_store_sales') }}
    where date_key between 20000101 and 20000131
),

items as (
    select *
    from {{ ref('dim_item') }}
)

select
    s.date_key,
    s.customer_id,
    s.item_id,
    s.quantity,
    s.sales_price,
    s.quantity * s.sales_price as revenue,
    i.category
from sales s
left join items i
    on s.item_id = i.item_id
