{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('tpch', 'CUSTOMER') }}
),
renamed as (
    select
        c_custkey as customer_id,
        c_name as customer_name,
        c_acctbal as account_balance,
        c_nationkey as nation_id
    from source
)
select * from renamed
