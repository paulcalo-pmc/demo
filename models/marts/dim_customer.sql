{{ config(
    materialized='table',
    tags=['mart', 'dimension']
) }}

with c as (
    select * from {{ ref('stg_tpch_customers') }}
),
summary as (
    select * from {{ ref('int_customer_order_summary') }}
)

select
    c.customer_id,
    c.customer_name,
    c.account_balance,
    coalesce(summary.total_orders, 0) as total_orders,
    coalesce(summary.total_quantity, 0) as total_quantity
from c
left join summary
    on c.customer_id = summary.customer_id
