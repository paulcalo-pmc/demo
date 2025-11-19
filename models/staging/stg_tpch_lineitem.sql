{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('tpch', 'LINEITEM') }}
),
renamed as (
    select
        l_orderkey as order_id,
        l_linenumber as line_number,
        l_partkey as part_id,
        l_quantity as quantity,
        l_extendedprice as extended_price
    from source
)
select * from renamed
