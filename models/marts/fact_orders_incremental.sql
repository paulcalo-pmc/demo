{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    tags=['mart', 'fact']
) }}

-- Simple incremental model for demonstration.
-- ---------------------------------------------------------------
-- Behaviour:
-- • First run: loads *all* orders.
-- • Incremental runs: only bring in rows where order_date is
--   greater than the max order_date already in the target table.
--
-- This keeps the model small, fast, and very easy for new users 
-- to understand.

with src as (

    select
        order_id,
        customer_id,
        order_date,
        total_price,
        order_status
    from {{ ref('stg_tpch_orders') }}

    {% if is_incremental() %}
        -- Only new records based on the order_date
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select
    order_id,
    customer_id,
    order_date,
    total_price,
    order_status,
    current_timestamp() as load_timestamp   -- helpful for demo visibility
from src