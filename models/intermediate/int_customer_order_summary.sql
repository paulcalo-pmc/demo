{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with details as (
    select * from {{ ref('int_order_details') }}
)

select
    customer_id,
    count(distinct order_id) as total_orders,
    sum(quantity) as total_quantity
from details
group by customer_id
