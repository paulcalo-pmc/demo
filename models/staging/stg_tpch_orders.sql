{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('tpch', 'ORDERS') }}
),
renamed as (
    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderdate as order_date,
        o_totalprice as total_price,
        o_orderstatus as order_status
    from source
)
select * from renamed
