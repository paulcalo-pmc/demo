{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with orders as (
    select * from {{ ref('stg_tpch_orders') }}
),
lineitems as (
    select * from {{ ref('stg_tpch_lineitem') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    li.part_id,
    li.quantity,
    li.extended_price
from orders o
join lineitems li on o.order_id = li.order_id
